package onebot

import "testing"

func BenchmarkParseEventGroupMessage(b *testing.B) {
	raw := []byte(`{"post_type":"message","message_type":"group","self_id":100000001,"group_id":123456789,"user_id":100000002,"message_id":99887766,"time":1760000000,"sender":{"user_id":100000002,"nickname":"Alice","card":"AliceCard","role":"member"},"message":[{"type":"at","data":{"qq":"100000001"}},{"type":"text","data":{"text":" 帮我看看明天的天气和机票"}},{"type":"face","data":{"id":"179"}}]}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := ParseEvent(raw, "100000001")
		if msg == nil || !msg.MentionedBot {
			b.Fatalf("unexpected parse result: %+v", msg)
		}
	}
}
