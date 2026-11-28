package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/iMMIQ/fatfish/internal/agent"
	"github.com/iMMIQ/fatfish/internal/config"
	"github.com/iMMIQ/fatfish/internal/console"
	"github.com/iMMIQ/fatfish/internal/cron"
	"github.com/iMMIQ/fatfish/internal/frame"
	"github.com/iMMIQ/fatfish/internal/infer"
	"github.com/iMMIQ/fatfish/internal/kb"
	"github.com/iMMIQ/fatfish/internal/memory"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/parser"
	"github.com/iMMIQ/fatfish/internal/policy"
	"github.com/iMMIQ/fatfish/internal/room"
	"github.com/iMMIQ/fatfish/internal/sticker"
	"github.com/iMMIQ/fatfish/internal/store"
	"github.com/iMMIQ/fatfish/internal/tool"
	"github.com/iMMIQ/fatfish/internal/transport"
	"github.com/iMMIQ/fatfish/internal/transport/onebot"
	"github.com/iMMIQ/fatfish/internal/travel"
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
	Travel   *travel.Service
	KB       *kb.Service
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
	memSvc.SetIgnoredBots(cfg.Policy.IgnoredBots)
	stkEng := sticker.NewEngine(st, hub, cfg.Sticker, cfg.Models.Embed.Dim, logger)
	travelSvc := travel.NewService(st, hub, cfg.Travel, cfg.Location(), cfg.PersonaText, cfg.Tools.ChartFontPath, cfg.Tools.SSRFAllowHosts, logger)
	var kbSvc *kb.Service
	if cfg.KB.Enabled && cfg.KB.Path != "" {
		kbSvc = kb.NewService(cfg.KB.Path, hub, logger)
		if kbSvc.Enabled() {
			if meta, merr := kbSvc.Meta(context.Background()); merr == nil {
				logger.Info("mounted read-only reference kb", "path", cfg.KB.Path, "books", meta["total_books"], "chunks", meta["total_chunks"])
			}
		} else {
			logger.Warn("kb enabled but file not found yet (will auto-mount on arrival)", "path", cfg.KB.Path)
		}
	}

	var tr transport.Transport
	if enableTransport && (cfg.Transport.OneBot.WSListen != "" || cfg.Transport.OneBot.WSURL != "") {
		tr = onebot.New(cfg.Transport.OneBot, logger)
	}

	reg := tool.NewRegistry(time.Duration(cfg.Tools.DefaultTimeoutSec) * time.Second)
	computeTool := tool.NewComputeTool(cfg.Tools.ChartFontPath, cfg.Tools.StarlarkMaxSteps, cfg.Tools.StarlarkTimeoutSec)

	// Single Scheduler instance wired to RoomManager via late-bound dispatcher closure
	// so ScheduleTool and App.Sched always share the exact same live Scheduler.
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
	sched.SetMediaQuotas(cfg.Frame.RoomMediaMiB, cfg.Frame.TotalMediaMiB)

	// Register all 10 high-leverage built-in tools (SPEC 10.2).
	linkParser := parser.NewService(buildParserOptions(cfg))
	webSearchTool := tool.NewWebSearchTool(cfg.Tools.SSRFAllowHosts)
	readSourceTool := tool.NewReadSourceTool(hub, cfg.Tools.SSRFAllowHosts)
	readSourceTool.SetParser(linkParser)
	quoteTool := tool.NewQuoteTool(cfg.Tools.SSRFAllowHosts, computeTool.Renderer())
	sched.SetPriceFetcher(quoteTool)
	weatherTool := tool.NewWeatherTool(cfg.Tools.SSRFAllowHosts)
	interactTool := tool.NewInteractTool(tr, cfg.Tools.AllowRelayChannels)
	if err := registerBuiltins(reg, st, memSvc, stkEng, sched, computeTool, webSearchTool, readSourceTool, quoteTool, weatherTool, interactTool, travelSvc, cfg.Travel.Enabled, kbSvc, cfg.KB); err != nil {
		_ = st.Close()
		return nil, err
	}

	// Connect MCP servers only for live runtime (serve), skipping network calls during offline CLI commands.
	mcpBridge := tool.NewMCPBridge(st, logger)
	mcpBridge.SetHub(hub)
	if enableTransport && len(cfg.MCP.Servers) > 0 {
		initCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		mcpBridge.InitAndRegister(initCtx, cfg.MCP.Servers, reg)
		cancel()
	}

	// Freeze sorted OpenAI tool declarations for 100% KV Prefix Cache stability.
	reg.Freeze()

	runner := agent.NewRunner(hub, st, fb, reg, logger)
	if enableTransport {
		runner.EnableAsyncTelemetry()
	}
	roomDisp = room.NewManager(room.Dependencies{
		Config:    cfgMgr,
		Store:     st,
		Policy:    pol,
		Hub:       hub,
		Frames:    fb,
		Runner:    runner,
		Memory:    memSvc,
		Stickers:  stkEng,
		Travel:    travelSvc,
		Registry:  reg,
		Parser:    linkParser,
		Transport: tr,
		Logger:    logger,
	})
	sched.SetMemoryHooks(memSvc, pol)
	travelSvc.SetHooks(roomDisp, pol)
	sched.SetTravelHooks(travelSvc)
	sched.SetSegmentHooks(roomDisp)

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
			Travel:   travelSvc,
			Logs:     logBuf,
		})
		if err != nil {
			_ = st.Close()
			return nil, err
		}
	}

	// Wire comprehensive atomic config hot-reload callbacks across all subsystems.
	cfgMgr.OnReload(func(newCfg *config.Config) {
		if err := pol.Update(newCfg.Policy); err != nil {
			logger.Error("policy reload failed", "err", err)
			return
		}
		hub.UpdateConfig(newCfg.Models)
		fb.UpdateConfig(newCfg.Frame)
		memSvc.SetIgnoredBots(newCfg.Policy.IgnoredBots)
		stkEng.UpdateConfig(newCfg.Sticker)
		travelSvc.UpdateConfig(newCfg.Travel, newCfg.Location(), newCfg.PersonaText, newCfg.Tools.ChartFontPath, newCfg.Tools.SSRFAllowHosts)
		if kbSvc != nil {
			kbSvc.UpdatePath(newCfg.KB.Path)
		}
		sched.SetMediaQuotas(newCfg.Frame.RoomMediaMiB, newCfg.Frame.TotalMediaMiB)
		sched.UpdateConfig(newCfg.Location(), newCfg.Tools.MaxSchedulesPerChannel, newCfg.Tools.MinCronIntervalMinutes)
		reg.UpdateDefaultTimeout(time.Duration(newCfg.Tools.DefaultTimeoutSec) * time.Second)
		roomDisp.UpdateHTTPClient(newCfg.Tools.SSRFAllowHosts)
		linkParser.UpdateOptions(buildParserOptions(newCfg))
		webSearchTool.UpdateAllowHosts(newCfg.Tools.SSRFAllowHosts)
		readSourceTool.UpdateAllowHosts(newCfg.Tools.SSRFAllowHosts)
		quoteTool.UpdateAllowHosts(newCfg.Tools.SSRFAllowHosts)
		weatherTool.UpdateAllowHosts(newCfg.Tools.SSRFAllowHosts)
		interactTool.UpdateAllowRelayChannels(newCfg.Tools.AllowRelayChannels)
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
		Travel:   travelSvc,
		KB:       kbSvc,
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

func buildParserOptions(cfg *config.Config) parser.Options {
	return parser.Options{
		AllowHosts:     cfg.Tools.SSRFAllowHosts,
		Timeout:        time.Duration(cfg.LinkParser.TimeoutSec) * time.Second,
		CacheTTL:       time.Duration(cfg.LinkParser.CacheTTLSec) * time.Second,
		TwitterAPIBase: cfg.LinkParser.TwitterAPIBase,
		GitHubToken:    cfg.LinkParser.GitHubToken,
		BilibiliCookie: cfg.LinkParser.BilibiliCookie,
		IgnoreDomains:  cfg.LinkParser.IgnoreDomains,
	}
}

func registerBuiltins(
	reg *tool.Registry,
	st *store.Store,
	mem *memory.Service,
	stk *sticker.Engine,
	sched *cron.Scheduler,
	comp *tool.ComputeTool,
	webSearch *tool.WebSearchTool,
	readSource *tool.ReadSourceTool,
	quote *tool.QuoteTool,
	weather *tool.WeatherTool,
	interact *tool.InteractTool,
	travelSvc *travel.Service,
	travelEnabled bool,
	kbSvc *kb.Service,
	kbCfg config.KBConfig,
) error {
	if err := tool.Register[tool.RecallArgs](reg, tool.NewRecallTool(mem)); err != nil {
		return err
	}
	if err := tool.Register[tool.NoteArgs](reg, tool.NewNoteTool(st)); err != nil {
		return err
	}
	if err := tool.Register[tool.WebSearchArgs](reg, webSearch); err != nil {
		return err
	}
	if err := tool.Register[tool.ReadSourceArgs](reg, readSource); err != nil {
		return err
	}
	if err := tool.Register[tool.ComputeArgs](reg, comp); err != nil {
		return err
	}
	if err := tool.Register[tool.QuoteArgs](reg, quote); err != nil {
		return err
	}
	if err := tool.Register[tool.WeatherArgs](reg, weather); err != nil {
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
	if err := tool.Register[tool.InteractArgs](reg, interact); err != nil {
		return err
	}
	if travelEnabled && travelSvc != nil {
		if err := tool.Register[tool.TravelArgs](reg, tool.NewTravelTool(travelSvc)); err != nil {
			return err
		}
	}
	if kbCfg.Enabled && kbSvc != nil {
		if err := tool.Register[tool.KBSearchArgs](reg, tool.NewKBSearchTool(kbSvc, kbCfg.MaxResults)); err != nil {
			return err
		}
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

	// Start scheduler and track exit so Store.Close() never races active maintenance/distill queries.
	var schedWG sync.WaitGroup
	schedWG.Add(1)
	go func() {
		defer schedWG.Done()
		a.Sched.Start(ctx)
	}()

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
	// 1. Stop accepting new inbound transport events while keeping outbound WS open for in-flight deliveries.
	if a.Trans != nil {
		_ = a.Trans.StopInbound()
	}
	// 2. Drain Rooms first (up to 12s), then drain any post-ACK async stickers and telemetry writes
	//    within the remaining 15s total drain budget.
	drainDeadline := time.Now().Add(15 * time.Second)
	a.Rooms.Drain(12 * time.Second)
	if a.Runner != nil {
		a.Runner.Drain(2 * time.Second)
	}
	if a.Stickers != nil {
		rem := time.Until(drainDeadline)
		if rem < 2*time.Second {
			rem = 2 * time.Second
		}
		a.Stickers.Drain(rem)
	}
	// 3. Wait for Scheduler goroutines (cancelled via ctx) to exit before closing SQLite pools.
	schedDone := make(chan struct{})
	go func() {
		schedWG.Wait()
		close(schedDone)
	}()
	select {
	case <-schedDone:
	case <-time.After(3 * time.Second):
		a.Logger.Warn("scheduler shutdown wait timed out")
	}
	// 4. Close transport socket, shutdown console, close MCP, flush WAL PASSIVE & close SQLite pools.
	if a.Trans != nil {
		_ = a.Trans.Stop()
	}
	if a.Console != nil {
		sdCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = a.Console.Shutdown(sdCtx)
		cancel()
	}
	if a.MCP != nil {
		a.MCP.Close()
	}
	if a.KB != nil {
		_ = a.KB.Close()
	}
	if err := a.Store.Close(); err != nil {
		return err
	}
	a.Logger.Info("fatfish shutdown complete")
	return nil
}

// Check runs all pre-flight diagnostics for `fatfish check` (SPEC §11.1):
// YAML + expr rules, bundled .ttf font, pdftotext, frozen tools, transport, inference endpoints, and MCP servers.
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

	// Probe OneBot transport connectivity/config.
	if cfg.Transport.OneBot.WSListen != "" || cfg.Transport.OneBot.WSURL != "" {
		tr := onebot.New(cfg.Transport.OneBot, a.Logger)
		if err := tr.Probe(ctx); err != nil {
			fmt.Printf("[WARN] transport onebot          : %v\n", err)
		} else {
			fmt.Printf("[OK] transport onebot ready\n")
		}
	}

	roles := []string{"primary", "fast", "distill", "embed", "rerank", "asr"}
	if cfg.Models.Translate.IsEnabled() {
		roles = append(roles, "translate")
	}
	for _, role := range roles {
		if err := a.Hub.ProbeEndpoint(ctx, role); err != nil {
			fmt.Printf("[WARN] inference endpoint %-9s: %v\n", role, err)
		} else {
			fmt.Printf("[OK] inference endpoint %-9s reachable\n", role)
		}
	}

	// Probe configured MCP servers.
	if len(cfg.MCP.Servers) > 0 && a.MCP != nil {
		for name, err := range a.MCP.Probe(ctx, cfg.MCP.Servers) {
			if err != nil {
				fmt.Printf("[WARN] mcp server %-14s: %v\n", name, err)
			} else {
				fmt.Printf("[OK] mcp server %-14s reachable\n", name)
			}
		}
	}
	return nil
}

// Close releases Store and MCP resources for CLI subcommands.
func (a *App) Close() {
	if a.MCP != nil {
		a.MCP.Close()
	}
	if a.KB != nil {
		_ = a.KB.Close()
	}
	if a.Store != nil {
		_ = a.Store.Close()
	}
}

type dispatcherFunc func(model.SystemTriggerEvent)

func (f dispatcherFunc) DispatchSystem(ev model.SystemTriggerEvent) { f(ev) }
