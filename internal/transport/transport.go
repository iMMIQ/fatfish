package transport

import (
	"context"
	"errors"

	"github.com/iMMIQ/fatfish/internal/model"
)

// ErrRiskMuted indicates platform anti-spam or group mute rejection (e.g. OneBot retcode 1200).
var ErrRiskMuted = errors.New("transport: message rejected due to mute or risk control")

// SendOptions controls optional reply quote and @mention decoration on outbound messages.
type SendOptions struct {
	ReplyToMsgID  string
	MentionUserID string
}

// ForwardNode represents a single node inside an outbound merged-forward card.
type ForwardNode struct {
	SenderID   string
	SenderName string
	Text       string
}

// Transport abstracts a messaging platform adapter (OneBot v11, Telegram, Discord, etc.).
type Transport interface {
	Start(ctx context.Context, onMessage func(*model.Message)) error
	// StopInbound stops accepting/dispatching new inbound events while keeping outbound
	// API calls functional during graceful shutdown drain (SPEC §11.3).
	StopInbound() error
	Stop() error
	Probe(ctx context.Context) error
	SelfID() string

	SendText(ctx context.Context, ch model.ChannelID, text string, opts SendOptions) (platformMsgID string, err error)
	SendImage(ctx context.Context, ch model.ChannelID, imgBytes []byte, mimeType string, opts SendOptions) (platformMsgID string, err error)
	SendForward(ctx context.Context, ch model.ChannelID, nodes []ForwardNode, summary string) (platformMsgID string, err error)
	SendPoke(ctx context.Context, ch model.ChannelID, targetUserID string) error
	SendReaction(ctx context.Context, ch model.ChannelID, platformMsgID string, emojiID string) error
	FetchAudio(ctx context.Context, fileID, outFormat string) (audioBytes []byte, filename string, err error)
	GetMessage(ctx context.Context, ch model.ChannelID, platformMsgID string) (*model.Message, error)
	GetForward(ctx context.Context, forwardID string, maxNodes int) ([]model.Part, error)
}
