package onebot

import (
	"testing"

	"github.com/iMMIQ/fatfish/internal/model"
)

func TestParseEventMultimodalAndPoke(t *testing.T) {
	raw := []byte(`{
		"post_type": "message",
		"message_type": "group",
		"self_id": 10001,
		"group_id": 99999,
		"user_id": 20001,
		"message_id": 888,
		"time": 1760000000,
		"sender": {"user_id": 20001, "nickname": "nick", "card": "Alice"},
		"message": [
			{"type": "at", "data": {"qq": "10001"}},
			{"type": "text", "data": {"text": " look at this"}},
			{"type": "image", "data": {"url": "https://example.com/a.jpg", "file": "a.jpg"}},
			{"type": "record", "data": {"file": "v.amr"}},
			{"type": "json", "data": {"data": "{\"prompt\":\"[B站]\",\"meta\":{\"detail_1\":{\"title\":\"Test Video\",\"desc\":\"Cool\",\"qqdocurl\":\"https://b23.tv/abc\"}}}"}}
		]
	}`)

	msg := ParseEvent(raw, "10001")
	if msg == nil {
		t.Fatal("expected parsed message")
	}
	if msg.ChannelID != "onebot:10001:group:99999" || !msg.MentionedBot || msg.SenderName != "Alice" {
		t.Fatalf("unexpected header: %+v", msg)
	}
	if len(msg.Parts) != 4 {
		t.Fatalf("expected 4 parts (text,image,audio,card), got %d: %+v", len(msg.Parts), msg.Parts)
	}
	if msg.Parts[2].Kind != model.PartAudio || msg.Parts[2].ASRStatus != "pending" {
		t.Fatalf("expected pending audio part, got %+v", msg.Parts[2])
	}
	if msg.Parts[3].Kind != model.PartCard || msg.Parts[3].URL != "https://b23.tv/abc" {
		t.Fatalf("expected parsed card, got %+v", msg.Parts[3])
	}

	pokeRaw := []byte(`{"post_type":"notice","notice_type":"notify","sub_type":"poke","self_id":10001,"group_id":99999,"user_id":20001,"target_id":10001}`)
	pMsg := ParseEvent(pokeRaw, "10001")
	if pMsg == nil || !pMsg.PokedBot {
		t.Fatalf("expected PokedBot=true, got %+v", pMsg)
	}

	// QQ native face, mface, and @other mention
	faceRaw := []byte(`{
		"post_type": "message",
		"message_type": "group",
		"self_id": 10001,
		"group_id": 99999,
		"user_id": 20001,
		"message_id": 889,
		"sender": {"user_id": 20001, "card": "Alice"},
		"message": [
			{"type": "at", "data": {"qq": "20002"}},
			{"type": "text", "data": {"text": " 真行"}},
			{"type": "face", "data": {"id": "179"}},
			{"type": "mface", "data": {"summary": "[捂脸哭]"}}
		]
	}`)
	fMsg := ParseEvent(faceRaw, "10001")
	if fMsg == nil || len(fMsg.Parts) != 4 {
		t.Fatalf("expected 4 parts for face/mface/mention, got %+v", fMsg)
	}
	if fMsg.Parts[0].Kind != model.PartMention || fMsg.Parts[0].TargetID != "20002" {
		t.Fatalf("expected PartMention for @other, got %+v", fMsg.Parts[0])
	}
	if fMsg.Parts[2].Text != "[表情:doge]" || fMsg.Parts[3].Text != "[捂脸哭]" {
		t.Fatalf("unexpected face/mface parts: %+v", fMsg.Parts)
	}

	// Silent group_recall and group_increase notices
	recallRaw := []byte(`{"post_type":"notice","notice_type":"group_recall","self_id":10001,"group_id":99999,"user_id":20001,"operator_id":20001,"message_id":888}`)
	rMsg := ParseEvent(recallRaw, "10001")
	if rMsg == nil || !rMsg.SilentNotice || len(rMsg.Parts) != 1 || rMsg.Parts[0].Kind != model.PartNotice || rMsg.Parts[0].RefMsgID != "888" {
		t.Fatalf("unexpected recall notice: %+v", rMsg)
	}
	joinRaw := []byte(`{"post_type":"notice","notice_type":"group_increase","self_id":10001,"group_id":99999,"user_id":20003}`)
	jMsg := ParseEvent(joinRaw, "10001")
	if jMsg == nil || !jMsg.SilentNotice || len(jMsg.Parts) != 1 || jMsg.Parts[0].Text != "[加入了本群]" {
		t.Fatalf("unexpected join notice: %+v", jMsg)
	}
}
