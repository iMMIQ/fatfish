package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/iMMIQ/fatfish/internal/agent"
	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/console"
	"github.com/iMMIQ/fatfish/internal/cron"
	"github.com/iMMIQ/fatfish/internal/frame"
	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/memory"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/policy"
	"github.com/iMMIQ/fatfish/internal/room"
	"github.com/iMMIQ/fatfish/internal/sticker"
	"github.com/iMMIQ/fatfish/internal/store"
	"github.com/iMMIQ/fatfish/internal/tool"
	"github.com/iMMIQ/fatfish/internal/transport"
	"github.com/iMMIQ/fatfish/internal/transport/onebot"
)

// App wires all fatfish subsystems together and manages lifecycle, hot-reload, and graceful drain.
type App struct {
	Config   *config.Manager
	Store    *store.Store
	Policy   *policy.Engine
	Hub      *infer.Hub
	Frames   *frame.Builder
	Memory   *memory.Service
	Stickers *sticker.Engine
	Registry *tool.Registry
	MCP      *tool.MCPBridge
	Runner   *agent.Runner
	Sched    *cron.Scheduler
	Rooms    *room.Manager
	Trans    transport.Transport
	Console  *console.Server
	Logs     *console.LogBroadcaster
	Logger   *slog.Logger
	Compute  *tool.ComputeTool
}

// New initializes all subsystems from configPath without starting background loops yet.
func New(configPath string, enableTransport bool) (*App, error) {
	logBuf := console.NewLogBroadcaster(os.Stdout, slog.LevelInfo, 300)
	logger := slog.New(logBuf)

	cfgMgr, err := config.NewManager(configPath, policy.ValidateConfig)
	if err != nil {
		return nil, err
	}
	cfg := cfgMgr.Get()

	tok, generated, err := config.EnsureConsoleToken(cfg)
	if err != nil {
		return nil, fmt.Errorf("app: console token: %w", err)
	}
	if generated {
		logger.Info("generated console token", "path", cfg.DataDir+"/.console_token")
	}

	st, err := store.Open(cfg.DataDir, cfg.Frame.ImageMaxDim, cfg.Frame.MaxImageMiB)
	if err != nil {
		return nil, err
	}

	pol, err := policy.NewEngine(cfg.Policy)
	if err != nil {
		_ = st.Close()
		return nil, err
	}

	hub := infer.NewHub(cfg.Models)
	fb := frame.NewBuilder(st, cfg.Frame)
	memSvc := memory.NewService(st, hub, cfg.Models.Embed.Dim, logger)
	stkEng := sticker.NewEngine(st, hub, cfg.Sticker, cfg.Models.Embed.Dim, logger)

	var tr transport.Transport
	if enableTransport && (cfg.Transport.OneBot.WSListen != "" || cfg.Transport.OneBot.WSURL != "") {
		tr = onebot.New(cfg.Transport.OneBot, logger)
	}

	reg := tool.NewRegistry(time.Duration(cfg.Tools.DefaultTimeoutSec) * time.Second)
	computeTool := tool.NewComputeTool(cfg.Tools.ChartFontPath, cfg.Tools.StarlarkMaxSteps, cfg.Tools.StarlarkTimeoutSec)

	// Scheduler needs room dispatcher; wire via late pointer after RoomManager creation.
	var roomDisp *room.Manager
	sched := cron.NewScheduler(
		st, hub,
		dispatcherFunc(func(ev model.SystemTriggerEvent) {
			if roomDisp != nil {
				roomDisp.DispatchSystem(ev)
			}
		}),
		cfg.Location(),
		cfg.Tools.MaxSchedulesPerChannel,
		cfg.Tools.MinCronIntervalMinutes,
		cfg.Models.Embed.Dim,
		logger,
	)

	// Register all 10 high-leverage built-in tools (SPEC 10.2).
	if err := registerBuiltins(reg, cfg, st, hub, memSvc, stkEng, sched, tr, computeTool); err != nil {
		_ = st.Close()
		return nil, err
	}

	// Connect MCP servers only for live runtime (serve), skipping network calls during offline CLI commands.
	mcpBridge := tool.NewMCPBridge(st, logger)
	if enableTransport && len(cfg.MCP.Servers) > 0 {
		initCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		mcpBridge.InitAndRegister(initCtx, cfg.MCP.Servers, reg)
		cancel()
	}

	// Freeze sorted OpenAI tool declarations for 100% KV Prefix Cache stability.
	reg.Freeze()

	runner := agent.NewRunner(hub, st, fb, reg, logger)
	roomDisp = room.NewManager(room.Dependencies{
		Config:    cfgMgr,
		Store:     st,
		Policy:    pol,
		Hub:       hub,
		Frames:    fb,
		Runner:    runner,
		Memory:    memSvc,
		Stickers:  stkEng,
		Registry:  reg,
		Transport: tr,
		Logger:    logger,
	})
	// Re-create scheduler bound directly to RoomManager so cron posts SystemTriggerEvent into Rooms.
	sched = cron.NewScheduler(
		st, hub, roomDisp, cfg.Location(),
		cfg.Tools.MaxSchedulesPerChannel,
		cfg.Tools.MinCronIntervalMinutes,
		cfg.Models.Embed.Dim,
		logger,
	)
	sched.SetMemoryHooks(memSvc, pol)

	var conSrv *console.Server
	if cfg.Console.Enabled {
		conSrv, err = console.NewServer(console.Dependencies{
			Listen:   cfg.Console.Listen,
			Token:    tok,
			Config:   cfgMgr,
			Store:    st,
			Hub:      hub,
			Rooms:    roomDisp,
			Registry: reg,
			Stickers: stkEng,
			Logs:     logBuf,
		})
		if err != nil {
			_ = st.Close()
			return nil, err
		}
	}

	// Wire atomic config hot-reload callbacks.
	cfgMgr.OnReload(func(newCfg *config.Config) {
		if err := pol.Update(newCfg.Policy); err != nil {
			logger.Error("policy reload failed", "err", err)
			return
		}
		hub.UpdateConfig(newCfg.Models)
		logger.Info("configuration hot-reloaded")
	})

	return &App{
		Config:   cfgMgr,
		Store:    st,
		Policy:   pol,
		Hub:      hub,
		Frames:   fb,
		Memory:   memSvc,
		Stickers: stkEng,
		Registry: reg,
		MCP:      mcpBridge,
		Runner:   runner,
		Sched:    sched,
		Rooms:    roomDisp,
		Trans:    tr,
		Console:  conSrv,
		Logs:     logBuf,
		Logger:   logger,
		Compute:  computeTool,
	}, nil
}

func registerBuiltins(
	reg *tool.Registry,
	cfg *config.Config,
	st *store.Store,
	hub *infer.Hub,
	mem *memory.Service,
	stk *sticker.Engine,
	sched *cron.Scheduler,
	tr transport.Transport,
	comp *tool.ComputeTool,
) error {
	if err := tool.Register[tool.RecallArgs](reg, tool.NewRecallTool(mem)); err != nil {
		return err
	}
	if err := tool.Register[tool.NoteArgs](reg, tool.NewNoteTool(st)); err != nil {
		return err
	}
	if err := tool.Register[tool.WebSearchArgs](reg, tool.NewWebSearchTool(cfg.Tools.SSRFAllowHosts)); err != nil {
		return err
	}
	if err := tool.Register[tool.ReadSourceArgs](reg, tool.NewReadSourceTool(hub, cfg.Tools.SSRFAllowHosts)); err != nil {
		return err
	}
	if err := tool.Register[tool.ComputeArgs](reg, comp); err != nil {
		return err
	}
	if err := tool.Register[tool.QuoteArgs](reg, tool.NewQuoteTool(cfg.Tools.SSRFAllowHosts)); err != nil {
		return err
	}
	if err := tool.Register[tool.ScheduleArgs](reg, tool.NewScheduleTool(sched, st)); err != nil {
		return err
	}
	if err := tool.Register[tool.SendStickerArgs](reg, tool.NewSendStickerTool(stk, st)); err != nil {
		return err
	}
	if err := tool.Register[tool.SendMediaArgs](reg, tool.NewSendMediaTool(st)); err != nil {
		return err
	}
	if err := tool.Register[tool.InteractArgs](reg, tool.NewInteractTool(tr, cfg.Tools.AllowRelayChannels)); err != nil {
		return err
	}
	return nil
}

// Serve runs transport, rooms, scheduler, config watcher, and console until SIGTERM/SIGINT,
// then drains in-flight work for up to 15s and executes WAL PASSIVE checkpoint.
func (a *App) Serve(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Load float16 vector indices into RAM.
	if n, err := a.Memory.LoadIndex(ctx); err == nil {
		a.Logger.Info("loaded episode vectors", "count", n)
	}
	if n, err := a.Stickers.LoadIndex(ctx); err == nil {
		a.Logger.Info("loaded sticker vectors", "count", n)
	}

	// Start config hot-reload watcher.
	if err := a.Config.Watch(ctx, func(err error) {
		a.Logger.Warn("config hot-reload rejected", "err", err)
	}); err != nil {
		a.Logger.Warn("config fsnotify watch disabled", "err", err)
	}

	// Start scheduler.
	go a.Sched.Start(ctx)

	// Start embedded HTMX console.
	if a.Console != nil {
		go func() {
			a.Logger.Info("admin console listening", "addr", a.Config.Get().Console.Listen)
			if err := a.Console.Start(); err != nil {
				a.Logger.Error("console server error", "err", err)
			}
		}()
	}

	// Start OneBot transport.
	if a.Trans != nil {
		if err := a.Trans.Start(ctx, a.Rooms.Dispatch); err != nil {
			return err
		}
	}

	a.Logger.Info("fatfish started", "tools", len(a.Registry.Definitions()))

	<-ctx.Done()
	a.Logger.Info("shutdown signal received, draining up to 15s...")

	// Graceful Shutdown sequence (SPEC 11.3):
	// 1. Close inbound transport reception.
	if a.Trans != nil {
		_ = a.Trans.Stop()
	}
	// 2. Wait up to 15s for in-flight LLM turns & bubble deliveries.
	a.Rooms.Drain(15 * time.Second)
	// 3. Shutdown console, close MCP, flush WAL PASSIVE & close SQLite pools.
	if a.Console != nil {
		sdCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = a.Console.Shutdown(sdCtx)
		cancel()
	}
	if a.MCP != nil {
		a.MCP.Close()
	}
	if err := a.Store.Close(); err != nil {
		return err
	}
	a.Logger.Info("fatfish shutdown complete")
	return nil
}

// Check runs all pre-flight diagnostics for `fatfish check`.
func (a *App) Check(ctx context.Context) error {
	cfg := a.Config.Get()
	fmt.Printf("[OK] YAML config parsed & %d policy expr rules compiled\n", 3+len(cfg.Policy.Overrides))

	if err := a.Compute.Renderer().VerifyFont(); err != nil {
		return fmt.Errorf("[FAIL] chart font (%s): %w", cfg.Tools.ChartFontPath, err)
	}
	fmt.Printf("[OK] TrueType CJK font loaded: %s\n", cfg.Tools.ChartFontPath)

	if p, err := exec.LookPath("pdftotext"); err != nil {
		fmt.Printf("[WARN] pdftotext not found in PATH (PDF extraction disabled)\n")
	} else {
		fmt.Printf("[OK] pdftotext available: %s\n", p)
	}

	fmt.Printf("[OK] Frozen tool registry ready (%d tools, hash=%s)\n", len(a.Registry.Definitions()), a.Registry.Hash()[:12])

	for _, role := range []string{"primary", "fast", "distill", "embed", "rerank", "asr"} {
		if err := a.Hub.ProbeEndpoint(ctx, role); err != nil {
			fmt.Printf("[WARN] inference endpoint %-8s: %v\n", role, err)
		} else {
			fmt.Printf("[OK] inference endpoint %-8s reachable\n", role)
		}
	}
	return nil
}

// Close releases Store and MCP resources for CLI subcommands.
func (a *App) Close() {
	if a.MCP != nil {
		a.MCP.Close()
	}
	if a.Store != nil {
		_ = a.Store.Close()
	}
}

type dispatcherFunc func(model.SystemTriggerEvent)

func (f dispatcherFunc) DispatchSystem(ev model.SystemTriggerEvent) { f(ev) }
