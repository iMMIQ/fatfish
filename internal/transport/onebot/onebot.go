package onebot

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/transport"
)

// Client implements transport.Transport over OneBot v11 WebSocket (NapCat compatible).
// The WS reader goroutine strictly decodes and dispatches without performing any heavy I/O.
type Client struct {
	cfg     config.OneBotConfig
	log     *slog.Logger
	selfID  atomic.Value // string
	echoSeq atomic.Int64

	connMu  sync.Mutex
	conn    *websocket.Conn
	writeMu sync.Mutex
	revSrv  *http.Server

	pendingMu sync.Mutex
	pending   map[string]chan apiResponse

	inboundStopped atomic.Bool
	stopOnce       sync.Once
	stopCh         chan struct{}
}

type apiResponse struct {
	Status  string          `json:"status"`
	RetCode int             `json:"retcode"`
	Data    json.RawMessage `json:"data"`
	Message string          `json:"message"`
	Wording string          `json:"wording"`
	Echo    string          `json:"echo"`
}

// New creates a OneBot v11 WebSocket transport client.
func New(cfg config.OneBotConfig, logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	c := &Client{
		cfg:     cfg,
		log:     logger,
		pending: make(map[string]chan apiResponse),
		stopCh:  make(chan struct{}),
	}
	c.selfID.Store(cfg.SelfID)
	return c
}

func (c *Client) SelfID() string {
	if v, ok := c.selfID.Load().(string); ok {
		return v
	}
	return c.cfg.SelfID
}

// Start runs OneBot v11 WS in Reverse Server mode (when ws_listen is set) or Forward Client mode (ws_url).
func (c *Client) Start(ctx context.Context, onMessage func(*model.Message)) error {
	if strings.TrimSpace(c.cfg.WSListen) != "" {
		return c.startReverseServer(ctx, onMessage)
	}
	if strings.TrimSpace(c.cfg.WSURL) == "" {
		return errors.New("onebot: neither ws_listen nor ws_url is configured")
	}
	go c.runLoop(ctx, onMessage)
	return nil
}

func (c *Client) startReverseServer(ctx context.Context, onMessage func(*model.Message)) error {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if c.cfg.Token != "" {
			auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			qTok := r.URL.Query().Get("access_token")
			if auth != c.cfg.Token && qTok != c.cfg.Token {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			c.log.Warn("onebot reverse ws upgrade failed", "err", err)
			return
		}
		if sid := r.Header.Get("X-Self-ID"); sid != "" {
			c.selfID.Store(sid)
		}
		c.connMu.Lock()
		if c.conn != nil {
			_ = c.conn.Close()
		}
		c.conn = conn
		c.connMu.Unlock()
		c.log.Info("onebot reverse ws client connected", "remote", r.RemoteAddr, "self_id", c.SelfID())

		c.readLoop(ctx, conn, onMessage)

		c.connMu.Lock()
		isCurrent := c.conn == conn
		if isCurrent {
			c.conn = nil
		}
		c.connMu.Unlock()
		_ = conn.Close()
		if isCurrent {
			c.failAllPending()
		}
	})
	srv := &http.Server{
		Addr:              c.cfg.WSListen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	c.connMu.Lock()
	c.revSrv = srv
	c.connMu.Unlock()

	go func() {
		c.log.Info("onebot reverse ws listening", "addr", c.cfg.WSListen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			c.log.Error("onebot reverse ws server error", "err", err)
		}
	}()
	return nil
}

// StopInbound stops dispatching new inbound platform events while keeping the active
// WebSocket connection open for outbound graceful drain replies (SPEC §11.3).
func (c *Client) StopInbound() error {
	c.inboundStopped.Store(true)
	return nil
}

func (c *Client) Stop() error {
	c.inboundStopped.Store(true)
	c.stopOnce.Do(func() {
		close(c.stopCh)
		c.connMu.Lock()
		if c.revSrv != nil {
			sdCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = c.revSrv.Shutdown(sdCtx)
			cancel()
		}
		if c.conn != nil {
			_ = c.conn.Close()
		}
		c.connMu.Unlock()
	})
	return nil
}

// Probe verifies transport configuration / endpoint reachability for `fatfish check`.
func (c *Client) Probe(ctx context.Context) error {
	if strings.TrimSpace(c.cfg.WSListen) != "" {
		return nil // Reverse WS server mode: validated by listen address config.
	}
	if strings.TrimSpace(c.cfg.WSURL) == "" {
		return errors.New("onebot: neither ws_listen nor ws_url is configured")
	}
	headers := http.Header{}
	if c.cfg.Token != "" {
		headers.Set("Authorization", "Bearer "+c.cfg.Token)
	}
	dialer := websocket.Dialer{
		HandshakeTimeout: 5 * time.Second,
		Proxy:            nil,
	}
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, _, err := dialer.DialContext(probeCtx, c.cfg.WSURL, headers)
	if err != nil {
		return err
	}
	_ = conn.Close()
	return nil
}

func (c *Client) runLoop(ctx context.Context, onMessage func(*model.Message)) {
	minBackoff := time.Duration(maxInt(c.cfg.ReconnectMinSec, 1)) * time.Second
	maxBackoff := time.Duration(maxInt(c.cfg.ReconnectMaxSec, 30)) * time.Second
	backoff := minBackoff

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		headers := http.Header{}
		if c.cfg.Token != "" {
			headers.Set("Authorization", "Bearer "+c.cfg.Token)
		}
		dialer := websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
			Proxy:            nil,
		}
		conn, _, err := dialer.DialContext(ctx, c.cfg.WSURL, headers)
		if err != nil {
			c.log.Warn("onebot ws dial failed", "err", err, "retry_in", backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		backoff = minBackoff
		c.connMu.Lock()
		c.conn = conn
		c.connMu.Unlock()
		c.log.Info("onebot ws connected", "url", c.cfg.WSURL)

		c.readLoop(ctx, conn, onMessage)

		c.connMu.Lock()
		isCurrent := c.conn == conn
		if isCurrent {
			c.conn = nil
		}
		c.connMu.Unlock()
		_ = conn.Close()
		if isCurrent {
			c.failAllPending()
		}
	}
}

func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn, onMessage func(*model.Message)) {
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				c.log.Warn("onebot ws read closed", "err", err)
			}
			return
		}
		c.handleFrame(payload, onMessage)
	}
}

func (c *Client) handleFrame(raw []byte, onMessage func(*model.Message)) {
	// Check if it's an API response (has echo).
	var probe struct {
		Echo     string `json:"echo"`
		PostType string `json:"post_type"`
		SelfID   any    `json:"self_id"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return
	}
	if sid := anyToString(probe.SelfID); sid != "" && c.SelfID() == "" {
		c.selfID.Store(sid)
	}
	if probe.Echo != "" && probe.PostType == "" {
		var resp apiResponse
		if err := json.Unmarshal(raw, &resp); err == nil {
			c.pendingMu.Lock()
			ch := c.pending[resp.Echo]
			delete(c.pending, resp.Echo)
			c.pendingMu.Unlock()
			if ch != nil {
				ch <- resp
			}
		}
		return
	}

	// Non-blocking event decode & dispatch (skip new inbound events when draining).
	if c.inboundStopped.Load() {
		return
	}
	if msg := ParseEvent(raw, c.SelfID()); msg != nil && onMessage != nil {
		onMessage(msg)
	}
}

// ParseEvent decodes a OneBot v11 JSON event into a normalized model.Message.
// Exported for unit tests.
func ParseEvent(raw []byte, fallbackSelfID string) *model.Message {
	var ev struct {
		PostType    string          `json:"post_type"`
		MessageType string          `json:"message_type"`
		NoticeType  string          `json:"notice_type"`
		SubType     string          `json:"sub_type"`
		SelfID      any             `json:"self_id"`
		GroupID     any             `json:"group_id"`
		UserID      any             `json:"user_id"`
		OperatorID  any             `json:"operator_id"`
		TargetID    any             `json:"target_id"`
		MessageID   any             `json:"message_id"`
		Time        int64           `json:"time"`
		RawMessage  string          `json:"raw_message"`
		Message     json.RawMessage `json:"message"`
		Sender      struct {
			UserID   any    `json:"user_id"`
			Nickname string `json:"nickname"`
			Card     string `json:"card"`
			Role     string `json:"role"`
		} `json:"sender"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil
	}

	selfID := anyToString(ev.SelfID)
	if selfID == "" {
		selfID = fallbackSelfID
	}
	ts := time.Now()
	if ev.Time > 0 {
		ts = time.Unix(ev.Time, 0)
	}

	// Handle poke notices.
	if ev.PostType == "notice" && ev.NoticeType == "notify" && ev.SubType == "poke" {
		userID := anyToString(ev.UserID)
		targetID := anyToString(ev.TargetID)
		if userID == "" || userID == selfID {
			return nil
		}
		groupID := anyToString(ev.GroupID)
		var ch model.ChannelID
		if groupID != "" {
			ch = model.MakeChannelID("onebot", selfID, model.ChannelGroup, groupID)
		} else {
			ch = model.MakeChannelID("onebot", selfID, model.ChannelDM, userID)
		}
		return &model.Message{
			ChannelID:  ch,
			SenderID:   userID,
			SenderName: userID,
			PokedBot:   targetID == selfID,
			Parts: []model.Part{{
				Kind:     model.PartPoke,
				TargetID: targetID,
				Text:     "[戳一戳]",
			}},
			CreatedAt: ts,
		}
	}

	// Handle silent background group notices (recall, member join) so the bot notices room events.
	if ev.PostType == "notice" && ev.NoticeType == "group_recall" {
		userID := anyToString(ev.UserID)
		groupID := anyToString(ev.GroupID)
		if userID == "" || userID == selfID || groupID == "" {
			return nil
		}
		ch := model.MakeChannelID("onebot", selfID, model.ChannelGroup, groupID)
		return &model.Message{
			ChannelID:    ch,
			SenderID:     userID,
			SenderName:   userID,
			SilentNotice: true,
			Parts: []model.Part{{
				Kind:     model.PartNotice,
				RefMsgID: anyToString(ev.MessageID),
				TargetID: anyToString(ev.OperatorID),
				Text:     "[撤回了一条消息]",
			}},
			CreatedAt: ts,
		}
	}
	if ev.PostType == "notice" && ev.NoticeType == "group_increase" {
		userID := anyToString(ev.UserID)
		groupID := anyToString(ev.GroupID)
		if userID == "" || userID == selfID || groupID == "" {
			return nil
		}
		ch := model.MakeChannelID("onebot", selfID, model.ChannelGroup, groupID)
		return &model.Message{
			ChannelID:    ch,
			SenderID:     userID,
			SenderName:   userID,
			SilentNotice: true,
			Parts: []model.Part{{
				Kind: model.PartNotice,
				Text: "[加入了本群]",
			}},
			CreatedAt: ts,
		}
	}

	if ev.PostType != "message" && ev.PostType != "message_sent" {
		return nil
	}
	userID := anyToString(ev.UserID)
	groupID := anyToString(ev.GroupID)
	var ch model.ChannelID
	if ev.MessageType == "group" || groupID != "" {
		ch = model.MakeChannelID("onebot", selfID, model.ChannelGroup, groupID)
	} else {
		ch = model.MakeChannelID("onebot", selfID, model.ChannelDM, userID)
	}

	senderName := strings.TrimSpace(ev.Sender.Card)
	if senderName == "" {
		senderName = strings.TrimSpace(ev.Sender.Nickname)
	}
	if senderName == "" {
		senderName = userID
	}

	msg := &model.Message{
		ChannelID:     ch,
		PlatformMsgID: anyToString(ev.MessageID),
		SenderID:      userID,
		SenderName:    senderName,
		SenderRole:    strings.TrimSpace(ev.Sender.Role),
		FromBot:       userID == selfID || ev.PostType == "message_sent",
		CreatedAt:     ts,
	}

	// Parse OneBot v11 message segments array.
	var segs []struct {
		Type string         `json:"type"`
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(ev.Message, &segs); err == nil && len(segs) > 0 {
		for _, seg := range segs {
			switch seg.Type {
			case "text":
				if txt := anyToString(seg.Data["text"]); txt != "" {
					msg.Parts = append(msg.Parts, model.Part{Kind: model.PartText, Text: txt})
				}
			case "at":
				qq := anyToString(seg.Data["qq"])
				if qq == selfID {
					msg.MentionedBot = true
				} else if qq == "all" {
					msg.Parts = append(msg.Parts, model.Part{Kind: model.PartText, Text: "@全体成员"})
				} else if qq != "" {
					msg.Parts = append(msg.Parts, model.Part{Kind: model.PartMention, TargetID: qq, Text: "@" + qq})
				}
			case "face":
				if ftxt := formatQQFace(seg.Data); ftxt != "" {
					msg.Parts = append(msg.Parts, model.Part{Kind: model.PartText, Text: ftxt})
				}
			case "mface":
				sum := strings.TrimSpace(anyToString(seg.Data["summary"]))
				if sum == "" {
					sum = "[大表情]"
				}
				msg.Parts = append(msg.Parts, model.Part{Kind: model.PartText, Text: sum})
			case "reply":
				rid := anyToString(seg.Data["id"])
				if rid != "" {
					msg.ReplyToMsgID = rid
					msg.Parts = append(msg.Parts, model.Part{Kind: model.PartQuote, RefMsgID: rid})
				}
			case "image":
				u := anyToString(seg.Data["url"])
				if u == "" {
					u = anyToString(seg.Data["file"])
				}
				msg.Parts = append(msg.Parts, model.Part{
					Kind:   model.PartImage,
					URL:    u,
					FileID: anyToString(seg.Data["file"]),
				})
			case "record":
				durSec, _ := strconv.Atoi(anyToString(seg.Data["duration"]))
				if durSec <= 0 {
					durSec, _ = strconv.Atoi(anyToString(seg.Data["time"]))
				}
				msg.Parts = append(msg.Parts, model.Part{
					Kind:       model.PartAudio,
					URL:        anyToString(seg.Data["url"]),
					FileID:     anyToString(seg.Data["file"]),
					DurationMs: durSec * 1000,
					ASRStatus:  "pending",
				})
			case "video":
				msg.Parts = append(msg.Parts, model.Part{
					Kind:   model.PartVideo,
					URL:    anyToString(seg.Data["url"]),
					FileID: anyToString(seg.Data["file"]),
				})
			case "file":
				sz, _ := strconv.ParseInt(anyToString(seg.Data["file_size"]), 10, 64)
				msg.Parts = append(msg.Parts, model.Part{
					Kind:     model.PartFile,
					Title:    anyToString(seg.Data["file"]),
					URL:      anyToString(seg.Data["url"]),
					FileID:   anyToString(seg.Data["file_id"]),
					FileSize: sz,
				})
			case "json":
				if card := parseQQJSONCard(anyToString(seg.Data["data"])); card != nil {
					msg.Parts = append(msg.Parts, *card)
				}
			case "forward":
				msg.Parts = append(msg.Parts, model.Part{
					Kind:   model.PartForward,
					FileID: anyToString(seg.Data["id"]),
				})
			}
		}
	} else if ev.RawMessage != "" {
		msg.Parts = append(msg.Parts, model.Part{Kind: model.PartText, Text: ev.RawMessage})
	}

	if len(msg.Parts) == 0 && !msg.MentionedBot {
		return nil
	}
	return msg
}

func parseQQJSONCard(raw string) *model.Part {
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil
	}
	prompt := strings.TrimSpace(anyToString(m["prompt"]))
	var metaTitle, summary, jumpURL string
	urlPriority := 99
	pickURL := func(candidate string, prio int) {
		c := strings.TrimSpace(candidate)
		if (strings.HasPrefix(c, "http://") || strings.HasPrefix(c, "https://")) && prio < urlPriority {
			jumpURL = c
			urlPriority = prio
		}
	}
	if meta, ok := m["meta"].(map[string]any); ok {
		for _, v := range meta {
			sub, ok := v.(map[string]any)
			if !ok {
				continue
			}
			if t := strings.TrimSpace(anyToString(sub["title"])); t != "" && metaTitle == "" {
				metaTitle = t
			}
			if d := strings.TrimSpace(anyToString(sub["desc"])); d != "" && summary == "" {
				summary = d
			}
			// Strict priority: real jump URLs first; never treat cover `preview` image URL as the link target.
			pickURL(anyToString(sub["qqdocurl"]), 1)
			pickURL(anyToString(sub["jumpUrl"]), 2)
			pickURL(anyToString(sub["jump_url"]), 3)
			pickURL(anyToString(sub["url"]), 4)
		}
	}
	title := prompt
	if title == "" {
		title = metaTitle
	} else if metaTitle != "" && !strings.Contains(title, metaTitle) {
		// Preserve both prompt (e.g. "[QQ小程序]哔哩哔哩") and specific meta title when distinct.
		if strings.HasPrefix(title, "[") && strings.HasSuffix(title, "]") {
			title = title + " " + metaTitle
		}
	}
	if title == "" && summary == "" && jumpURL == "" {
		return nil
	}
	return &model.Part{
		Kind:    model.PartCard,
		Title:   title,
		Summary: summary,
		URL:     jumpURL,
	}
}

// --- Outbound OneBot v11 Actions ---

func (c *Client) callAPI(ctx context.Context, action string, params map[string]any) (apiResponse, error) {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return apiResponse{}, errors.New("onebot: not connected")
	}

	echo := fmt.Sprintf("ff_%d_%d", time.Now().UnixNano(), c.echoSeq.Add(1))
	ch := make(chan apiResponse, 1)
	c.pendingMu.Lock()
	c.pending[echo] = ch
	c.pendingMu.Unlock()

	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, echo)
		c.pendingMu.Unlock()
	}()

	reqPayload, err := json.Marshal(map[string]any{
		"action": action,
		"params": params,
		"echo":   echo,
	})
	if err != nil {
		return apiResponse{}, err
	}

	timeout := time.Duration(maxInt(c.cfg.APITimeoutSec, 10)) * time.Second
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	c.writeMu.Lock()
	writeDeadline := time.Now().Add(timeout)
	if dl, ok := reqCtx.Deadline(); ok && dl.Before(writeDeadline) {
		writeDeadline = dl
	}
	_ = conn.SetWriteDeadline(writeDeadline)
	err = conn.WriteMessage(websocket.TextMessage, reqPayload)
	c.writeMu.Unlock()
	if err != nil {
		return apiResponse{}, err
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return apiResponse{}, errors.New("onebot: connection closed while awaiting response")
		}
		if resp.RetCode == 1200 {
			return resp, fmt.Errorf("%w: %s", transport.ErrRiskMuted, resp.Wording)
		}
		if resp.RetCode != 0 {
			return resp, fmt.Errorf("onebot api %s failed: retcode=%d msg=%s %s", action, resp.RetCode, resp.Message, resp.Wording)
		}
		return resp, nil
	case <-reqCtx.Done():
		return apiResponse{}, reqCtx.Err()
	}
}

func (c *Client) failAllPending() {
	c.pendingMu.Lock()
	for k, ch := range c.pending {
		close(ch)
		delete(c.pending, k)
	}
	c.pendingMu.Unlock()
}

func (c *Client) SendText(ctx context.Context, ch model.ChannelID, text string, opts transport.SendOptions) (string, error) {
	segs := make([]map[string]any, 0, 3)
	if opts.ReplyToMsgID != "" {
		segs = append(segs, map[string]any{"type": "reply", "data": map[string]any{"id": opts.ReplyToMsgID}})
	}
	if opts.MentionUserID != "" && ch.IsGroup() {
		segs = append(segs, map[string]any{"type": "at", "data": map[string]any{"qq": opts.MentionUserID}})
		segs = append(segs, map[string]any{"type": "text", "data": map[string]any{"text": " " + text}})
	} else {
		segs = append(segs, map[string]any{"type": "text", "data": map[string]any{"text": text}})
	}
	return c.sendSegments(ctx, ch, segs)
}

func (c *Client) SendImage(ctx context.Context, ch model.ChannelID, imgBytes []byte, _ string, opts transport.SendOptions) (string, error) {
	segs := make([]map[string]any, 0, 2)
	if opts.ReplyToMsgID != "" {
		segs = append(segs, map[string]any{"type": "reply", "data": map[string]any{"id": opts.ReplyToMsgID}})
	}
	segs = append(segs, map[string]any{
		"type": "image",
		"data": map[string]any{"file": "base64://" + base64.StdEncoding.EncodeToString(imgBytes)},
	})
	return c.sendSegments(ctx, ch, segs)
}

// SendForward sends a merged-forward message card via OneBot `send_group_forward_msg` / `send_private_forward_msg`.
func (c *Client) SendForward(ctx context.Context, ch model.ChannelID, nodes []transport.ForwardNode, summary string) (string, error) {
	if len(nodes) == 0 {
		return "", errors.New("onebot: empty forward nodes")
	}
	_, _, kind, nativeID, ok := model.ParseChannelID(ch)
	if !ok {
		return "", fmt.Errorf("onebot: invalid channel id %q", ch)
	}
	selfID := c.SelfID()
	msgNodes := make([]map[string]any, 0, len(nodes))
	for _, n := range nodes {
		txt := strings.TrimSpace(n.Text)
		if txt == "" {
			continue
		}
		uin := strings.TrimSpace(n.SenderID)
		if uin == "" {
			uin = selfID
		}
		name := strings.TrimSpace(n.SenderName)
		if name == "" {
			name = "fatfish"
		}
		msgNodes = append(msgNodes, map[string]any{
			"type": "node",
			"data": map[string]any{
				"uin":  uin,
				"name": name,
				"content": []map[string]any{
					{"type": "text", "data": map[string]any{"text": txt}},
				},
			},
		})
	}
	if len(msgNodes) == 0 {
		return "", errors.New("onebot: no non-empty forward nodes")
	}

	params := map[string]any{"messages": msgNodes}
	if s := strings.TrimSpace(summary); s != "" {
		params["summary"] = s
	}
	var action string
	if kind == model.ChannelGroup {
		action = "send_group_forward_msg"
		if idNum, err := strconv.ParseInt(nativeID, 10, 64); err == nil {
			params["group_id"] = idNum
		} else {
			params["group_id"] = nativeID
		}
	} else {
		action = "send_private_forward_msg"
		if idNum, err := strconv.ParseInt(nativeID, 10, 64); err == nil {
			params["user_id"] = idNum
		} else {
			params["user_id"] = nativeID
		}
	}
	resp, err := c.callAPI(ctx, action, params)
	if err != nil {
		return "", err
	}
	var data struct {
		MessageID any `json:"message_id"`
	}
	_ = json.Unmarshal(resp.Data, &data)
	return anyToString(data.MessageID), nil
}

func (c *Client) sendSegments(ctx context.Context, ch model.ChannelID, segs []map[string]any) (string, error) {
	_, _, kind, nativeID, ok := model.ParseChannelID(ch)
	if !ok {
		return "", fmt.Errorf("onebot: invalid channel id %q", ch)
	}
	var action string
	params := map[string]any{"message": segs}
	if kind == model.ChannelGroup {
		action = "send_group_msg"
		if idNum, err := strconv.ParseInt(nativeID, 10, 64); err == nil {
			params["group_id"] = idNum
		} else {
			params["group_id"] = nativeID
		}
	} else {
		action = "send_private_msg"
		if idNum, err := strconv.ParseInt(nativeID, 10, 64); err == nil {
			params["user_id"] = idNum
		} else {
			params["user_id"] = nativeID
		}
	}

	resp, err := c.callAPI(ctx, action, params)
	if err != nil {
		return "", err
	}
	var data struct {
		MessageID any `json:"message_id"`
	}
	_ = json.Unmarshal(resp.Data, &data)
	return anyToString(data.MessageID), nil
}

func (c *Client) SendPoke(ctx context.Context, ch model.ChannelID, targetUserID string) error {
	_, _, kind, nativeID, ok := model.ParseChannelID(ch)
	if !ok {
		return fmt.Errorf("onebot: invalid channel id %q", ch)
	}
	uid, _ := strconv.ParseInt(targetUserID, 10, 64)
	if kind == model.ChannelGroup {
		gid, _ := strconv.ParseInt(nativeID, 10, 64)
		_, err := c.callAPI(ctx, "group_poke", map[string]any{"group_id": gid, "user_id": uid})
		return err
	}
	_, err := c.callAPI(ctx, "friend_poke", map[string]any{"user_id": uid})
	return err
}

func (c *Client) SendReaction(ctx context.Context, _ model.ChannelID, platformMsgID string, emojiID string) error {
	mid, _ := strconv.ParseInt(platformMsgID, 10, 64)
	_, err := c.callAPI(ctx, "set_msg_emoji_like", map[string]any{
		"message_id": mid,
		"emoji_id":   emojiID,
	})
	return err
}

// FetchAudio calls NapCat `get_record` to transcode SILK v3 to WAV and returns the WAV bytes.
func (c *Client) FetchAudio(ctx context.Context, fileID, outFormat string) ([]byte, string, error) {
	if outFormat == "" {
		outFormat = "wav"
	}
	resp, err := c.callAPI(ctx, "get_record", map[string]any{
		"file":       fileID,
		"out_format": outFormat,
	})
	if err != nil {
		return nil, "", err
	}
	var data struct {
		File   string `json:"file"`
		URL    string `json:"url"`
		Base64 string `json:"base64"`
	}
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		return nil, "", err
	}
	if data.Base64 != "" {
		raw, err := base64.StdEncoding.DecodeString(data.Base64)
		return raw, "voice." + outFormat, err
	}
	if data.File != "" {
		if raw, err := os.ReadFile(data.File); err == nil {
			return raw, "voice." + outFormat, nil
		}
	}
	if strings.HasPrefix(data.URL, "http") {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, data.URL, nil)
		if err != nil {
			return nil, "", err
		}
		hcli := &http.Client{
			Timeout:   10 * time.Second,
			Transport: &http.Transport{Proxy: nil},
		}
		hresp, err := hcli.Do(req)
		if err != nil {
			return nil, "", err
		}
		defer hresp.Body.Close()
		if hresp.StatusCode < 200 || hresp.StatusCode >= 300 {
			return nil, "", fmt.Errorf("onebot: fetch audio HTTP %d", hresp.StatusCode)
		}
		raw, err := io.ReadAll(io.LimitReader(hresp.Body, 16<<20))
		return raw, "voice." + outFormat, err
	}
	return nil, "", errors.New("onebot: get_record returned no readable audio data")
}

// GetForward fetches bounded forward message nodes (<=maxNodes, default 20) via OneBot `get_forward_msg`.
func (c *Client) GetForward(ctx context.Context, forwardID string, maxNodes int) ([]model.Part, error) {
	if strings.TrimSpace(forwardID) == "" {
		return nil, nil
	}
	if maxNodes <= 0 || maxNodes > 20 {
		maxNodes = 20
	}
	resp, err := c.callAPI(ctx, "get_forward_msg", map[string]any{"id": forwardID})
	if err != nil {
		return nil, err
	}
	var data struct {
		Messages []struct {
			Sender struct {
				Nickname string `json:"nickname"`
				UserID   any    `json:"user_id"`
			} `json:"sender"`
			Content json.RawMessage `json:"content"`
			Message json.RawMessage `json:"message"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		return nil, err
	}
	nodes := make([]model.Part, 0, min(len(data.Messages), maxNodes))
	for _, m := range data.Messages {
		if len(nodes) >= maxNodes {
			break
		}
		rawMsg := m.Message
		if len(rawMsg) == 0 {
			rawMsg = m.Content
		}
		var txt string
		var segs []struct {
			Type string         `json:"type"`
			Data map[string]any `json:"data"`
		}
		if json.Unmarshal(rawMsg, &segs) == nil && len(segs) > 0 {
			var parts []string
			for _, s := range segs {
				if s.Type == "text" {
					if t := strings.TrimSpace(anyToString(s.Data["text"])); t != "" {
						parts = append(parts, t)
					}
				} else if s.Type == "image" {
					parts = append(parts, "[图片]")
				} else if s.Type == "json" {
					if card := parseQQJSONCard(anyToString(s.Data["data"])); card != nil {
						parts = append(parts, strings.TrimSpace(fmt.Sprintf("[卡片: %s %s %s]", card.Title, card.Summary, card.URL)))
					}
				}
			}
			txt = strings.Join(parts, " ")
		} else {
			_ = json.Unmarshal(rawMsg, &txt)
		}
		txt = strings.TrimSpace(txt)
		if txt == "" {
			continue
		}
		sender := strings.TrimSpace(m.Sender.Nickname)
		if sender == "" {
			sender = anyToString(m.Sender.UserID)
		}
		nodes = append(nodes, model.Part{
			Kind: model.PartText,
			Text: fmt.Sprintf("%s: %s", sender, txt),
		})
	}
	return nodes, nil
}

func (c *Client) GetMessage(ctx context.Context, ch model.ChannelID, platformMsgID string) (*model.Message, error) {
	mid, _ := strconv.ParseInt(platformMsgID, 10, 64)
	resp, err := c.callAPI(ctx, "get_msg", map[string]any{"message_id": mid})
	if err != nil {
		return nil, err
	}
	// Wrap into a synthetic message event so ParseEvent can normalize it cleanly.
	var inner map[string]any
	if err := json.Unmarshal(resp.Data, &inner); err != nil {
		return nil, err
	}
	inner["post_type"] = "message"
	inner["self_id"] = c.SelfID()
	if ch.IsGroup() {
		inner["message_type"] = "group"
		inner["group_id"] = ch.NativeID()
	} else {
		inner["message_type"] = "private"
	}
	wrapped, _ := json.Marshal(inner)
	msg := ParseEvent(wrapped, c.SelfID())
	if msg == nil {
		return nil, errors.New("onebot: failed to parse get_msg payload")
	}
	return msg, nil
}

func anyToString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case json.Number:
		return x.String()
	case int64:
		return strconv.FormatInt(x, 10)
	case int:
		return strconv.Itoa(x)
	default:
		return fmt.Sprint(x)
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
