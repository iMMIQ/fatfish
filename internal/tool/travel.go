package tool

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/travel"
)

// TravelArgs defines the flat, $ref-free schema for the continuous world-travel tool.
type TravelArgs struct {
	Action string `json:"action" jsonschema:"required,enum=status,enum=history,enum=suggest_next,enum=map,enum=ticket,description=Operation: status (current city/weather/local time/recent diary), history (visited cities), suggest_next (record member suggestion for next stop), map (send world footprint map PNG), ticket (send boarding pass/postcard PNG)"`
	City   string `json:"city,omitempty" jsonschema:"description=Target city or country name for action=suggest_next (e.g. 东京, 里斯本, 冰岛, New York)"`
	Note   string `json:"note,omitempty" jsonschema:"description=Optional short reason/note from the member for action=suggest_next"`
	Limit  int    `json:"limit,omitempty" jsonschema:"description=Max visited cities to list for action=history (default 15, max 30)"`
}

// TravelTool exposes the bot's global continuous world-travel state, footprint history,
// channel-scoped destination suggestions, and pure-Go map/boarding-pass PNG rendering.
type TravelTool struct {
	svc *travel.Service
}

func NewTravelTool(svc *travel.Service) *TravelTool { return &TravelTool{svc: svc} }

func (t *TravelTool) Name() string { return "travel" }
func (t *TravelTool) ReadOnly() bool {
	return false
}
func (t *TravelTool) Description() string {
	return "Check your ongoing world-travel location/weather/diary, view visited city history, record a member's suggestion for the next destination, or send a world footprint map / boarding pass image."
}

func (t *TravelTool) Execute(ctx context.Context, tc ToolContext, args TravelArgs) (ToolResult, error) {
	if t.svc == nil || !t.svc.Enabled() {
		return ToolResult{TextForLLM: "World travel mode is currently disabled."}, nil
	}
	switch strings.ToLower(strings.TrimSpace(args.Action)) {
	case "status", "":
		txt, err := t.svc.DetailedStatus(ctx, tc.ChannelID, time.Now())
		if err != nil {
			return ToolResult{}, err
		}
		return ToolResult{TextForLLM: txt}, nil

	case "history":
		txt, err := t.svc.HistorySummary(ctx, args.Limit)
		if err != nil {
			return ToolResult{}, err
		}
		return ToolResult{TextForLLM: txt}, nil

	case "suggest_next":
		cityQuery := strings.TrimSpace(args.City)
		if cityQuery == "" {
			return ToolResult{}, errors.New("travel.suggest_next requires city")
		}
		sg, city, err := t.svc.SuggestNext(ctx, tc.ChannelID, tc.SenderID, tc.SenderName, cityQuery, args.Note)
		if err != nil {
			return ToolResult{TextForLLM: err.Error()}, nil
		}
		return ToolResult{
			TextForLLM: fmt.Sprintf("已记下本群成员 %s 的下一站提名：%s·%s (%s)，编号 #%d（下次换城市时会优先考虑）。",
				sg.MemberName, city.CountryZH, city.NameZH, city.AirportCode, sg.ID),
		}, nil

	case "map":
		pngBytes, err := t.svc.RenderMapPNG(ctx)
		if err != nil {
			return ToolResult{}, err
		}
		summary, _ := t.svc.HistorySummary(ctx, 5)
		return ToolResult{
			TextForLLM: "已生成并附带发送环球旅行足迹图。\n" + summary,
			DirectMedia: []model.DirectMedia{{
				Data:     pngBytes,
				MimeType: "image/png",
				Filename: "travel_map.png",
				Caption:  "环球旅行足迹图",
			}},
		}, nil

	case "ticket":
		pngBytes, err := t.svc.RenderBoardingPassPNG(ctx)
		if err != nil {
			return ToolResult{}, err
		}
		summary, _ := t.svc.DetailedStatus(ctx, tc.ChannelID, time.Now())
		return ToolResult{
			TextForLLM: "已生成并附带发送当前行程纪念票/登机牌。\n" + summary,
			DirectMedia: []model.DirectMedia{{
				Data:     pngBytes,
				MimeType: "image/png",
				Filename: "travel_ticket.png",
				Caption:  "环球旅行登机牌",
			}},
		}, nil

	default:
		return ToolResult{}, fmt.Errorf("unsupported travel action %q", args.Action)
	}
}
