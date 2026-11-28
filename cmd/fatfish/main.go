package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime/debug"

	"gopkg.in/yaml.v3"

	"github.com/iMMIQ/fatfish/internal/app"
	"github.com/iMMIQ/fatfish/internal/ingest"
	"github.com/iMMIQ/fatfish/internal/memory"
	"github.com/iMMIQ/fatfish/internal/model"
	"github.com/iMMIQ/fatfish/internal/sticker"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	sub := os.Args[1]
	args := os.Args[2:]

	var err error
	switch sub {
	case "serve":
		err = cmdServe(args)
	case "check":
		err = cmdCheck(args)
	case "ingest":
		err = cmdIngest(args)
	case "distill":
		err = cmdDistill(args)
	case "reindex":
		err = cmdReindex(args)
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", sub)
		usage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Println(`fatfish — Pure-Go Multimodal Group & Personal AI Assistant Runtime

Usage:
  fatfish serve   -c fatfish.yaml
  fatfish check   -c fatfish.yaml
  fatfish ingest  -c fatfish.yaml [--messages history.jsonl] [--stickers ./dir --pack default]
  fatfish distill -c fatfish.yaml [--channel <id>] [--limit 20] [--concurrency 2]
  fatfish reindex -c fatfish.yaml [--eval queries.yaml] [--enrich] [--tag]`)
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	cfgPath := fs.String("c", "configs/fatfish.example.yaml", "path to YAML config")
	_ = fs.Parse(args)

	// Default soft memory limit for 512 MiB container deployments unless overridden via GOMEMLIMIT.
	if os.Getenv("GOMEMLIMIT") == "" {
		debug.SetMemoryLimit(360 << 20)
	}

	application, err := app.New(*cfgPath, true)
	if err != nil {
		return err
	}
	return application.Serve(context.Background())
}

func cmdCheck(args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	cfgPath := fs.String("c", "configs/fatfish.example.yaml", "path to YAML config")
	_ = fs.Parse(args)

	application, err := app.New(*cfgPath, false)
	if err != nil {
		return err
	}
	defer application.Close()
	return application.Check(context.Background())
}

func cmdIngest(args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	cfgPath := fs.String("c", "configs/fatfish.example.yaml", "path to YAML config")
	msgFile := fs.String("messages", "", "path to generic messages .jsonl")
	stkDir := fs.String("stickers", "", "path to generic sticker pack directory")
	packID := fs.String("pack", "default", "sticker pack ID")
	_ = fs.Parse(args)

	if *msgFile == "" && *stkDir == "" {
		return fmt.Errorf("specify --messages and/or --stickers")
	}

	application, err := app.New(*cfgPath, false)
	if err != nil {
		return err
	}
	defer application.Close()

	ctx := context.Background()
	if *msgFile != "" {
		rep, err := ingest.MessagesJSONL(ctx, application.Store, *msgFile)
		if err != nil {
			return err
		}
		fmt.Printf("Messages ingested: total=%d inserted=%d skipped=%d\n", rep.Total, rep.Inserted, rep.Skipped)
	}
	if *stkDir != "" {
		rep, err := ingest.StickerDir(ctx, application.Store, *stkDir, application.Config.Get().Sticker.PackDir, *packID)
		if err != nil {
			return err
		}
		fmt.Printf("Stickers ingested: total=%d inserted=%d skipped=%d\n", rep.Total, rep.Inserted, rep.Skipped)
	}
	return nil
}

func cmdDistill(args []string) error {
	fs := flag.NewFlagSet("distill", flag.ExitOnError)
	cfgPath := fs.String("c", "configs/fatfish.example.yaml", "path to YAML config")
	channel := fs.String("channel", "", "optional canonical channel_id (defaults to all allowed channels)")
	limit := fs.Int("limit", 0, "max episodes per channel (0 = unlimited)")
	concurrency := fs.Int("concurrency", 2, "worker concurrency")
	sinceDays := fs.Int("since-days", 0, "only distill messages from the last N days (0 = all)")
	role := fs.String("role", "distill", "model role (distill|fast|primary)")
	_ = fs.Parse(args)

	application, err := app.New(*cfgPath, false)
	if err != nil {
		return err
	}
	defer application.Close()

	ctx := context.Background()
	var targets []model.ChannelID
	if *channel != "" {
		targets = []model.ChannelID{model.ChannelID(*channel)}
	} else {
		chs, err := application.Store.ListChannels(ctx)
		if err != nil {
			return err
		}
		for _, c := range chs {
			if !application.Policy.ChannelAllowed(c.ChannelID) {
				continue
			}
			targets = append(targets, c.ChannelID)
		}
	}

	total := 0
	for _, ch := range targets {
		n, err := application.Memory.DistillChannel(ctx, memory.DistillOptions{
			ChannelID:   ch,
			MaxEpisodes: *limit,
			Concurrency: *concurrency,
			Role:        *role,
			SinceDays:   *sinceDays,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "channel %s distill error: %v\n", ch, err)
			continue
		}
		if n > 0 {
			fmt.Printf("Distilled channel %s: +%d episode(s)\n", ch, n)
		}
		total += n
	}
	fmt.Printf("Total distilled episodes: %d\n", total)
	return nil
}

func cmdReindex(args []string) error {
	fs := flag.NewFlagSet("reindex", flag.ExitOnError)
	cfgPath := fs.String("c", "configs/fatfish.example.yaml", "path to YAML config")
	evalFile := fs.String("eval", "", "optional YAML file of sticker eval queries to tune alpha")
	enrich := fs.Bool("enrich", false, "enrich missing sticker intent_text via LLM")
	tag := fs.Bool("tag", false, "vision-tag untagged needs_review stickers via primary multimodal model")
	_ = fs.Parse(args)

	application, err := app.New(*cfgPath, false)
	if err != nil {
		return err
	}
	defer application.Close()

	ctx := context.Background()
	if *enrich || *tag {
		enr, tgd, err := application.Stickers.EnrichAndTag(ctx, *tag)
		if err != nil {
			fmt.Printf("[WARN] sticker enrich/tag skipped: %v\n", err)
		} else {
			fmt.Printf("[OK] Sticker enrichment complete (enriched=%d, vision_tagged=%d)\n", enr, tgd)
		}
	}
	if err := application.Store.RebuildFTS(ctx); err != nil {
		return fmt.Errorf("rebuild FTS5: %w", err)
	}
	fmt.Println("[OK] Rebuilt SQLite FTS5 tables")

	if n, err := application.Memory.ReindexEmbeddings(ctx); err != nil {
		fmt.Printf("[WARN] episode embedding reindex skipped: %v\n", err)
	} else {
		fmt.Printf("[OK] Episode vector index rebuilt (%d vectors)\n", n)
	}

	alpha := application.Config.Get().Sticker.DocCenteringAlpha
	if *evalFile != "" {
		raw, err := os.ReadFile(*evalFile)
		if err != nil {
			return err
		}
		var queries []sticker.EvalQuery
		if err := yaml.Unmarshal(raw, &queries); err != nil {
			return err
		}
		for _, a := range []float64{0.0, 0.5, 0.7, 1.0} {
			if _, err := application.Stickers.Reindex(ctx, a); err != nil {
				return err
			}
			rep := application.Stickers.EvaluateBenchmark(ctx, a, queries)
			fmt.Printf("alpha=%.1f  Recall@5=%.3f  UniqueTop1=%d/%d\n", rep.Alpha, rep.RecallAt5, rep.UniqueTop1, rep.Total)
		}
	}
	if n, err := application.Stickers.Reindex(ctx, alpha); err != nil {
		fmt.Printf("[WARN] sticker vector reindex skipped: %v\n", err)
	} else {
		fmt.Printf("[OK] Sticker dual-vector index rebuilt (%d stickers, alpha=%.2f)\n", n, alpha)
	}
	return nil
}
