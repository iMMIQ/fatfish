package tool

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	starjson "go.starlark.net/lib/json"
	starmath "go.starlark.net/lib/math"
	startime "go.starlark.net/lib/time"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
	"go.starlark.net/syntax"

	"github.com/iMMIQ/fatfish/internal/model"
)

const (
	maxStarlarkRepeatMul = 10000
	maxStarlarkRangeLen  = 20000
	maxStarlarkOutputLen = 64 << 10 // 64 KiB
	maxChartsPerCompute  = 4
)

// ErrStarlarkOOMBlocked is returned by the AST pre-validator when a script attempts an OOM DoS pattern.
var ErrStarlarkOOMBlocked = errors.New("compute: blocked unsafe allocation pattern in Starlark AST")

type ComputeArgs struct {
	Code string `json:"code" jsonschema:"required,description=Starlark script to execute. Preloaded: math, json, time, stats, finance, chart. Guide visuals: chart.card(title, sections=[{'heading':'Day1 ...','items':['...']}], subtitle='...', footer='避坑: ...'); chart.table(title, headers=['维度','方案A','方案B'], rows=[['预算','...','...']], subtitle='...', highlight_col=1). Use print() for text output."`
}

type ComputeTool struct {
	renderer *ChartRenderer
	maxSteps uint64
	timeout  time.Duration
}

func NewComputeTool(fontPath string, maxSteps int, timeoutSec int) *ComputeTool {
	if maxSteps <= 0 {
		maxSteps = 500000
	}
	if timeoutSec <= 0 {
		timeoutSec = 3
	}
	return &ComputeTool{
		renderer: NewChartRenderer(fontPath),
		maxSteps: uint64(maxSteps),
		timeout:  time.Duration(timeoutSec) * time.Second,
	}
}

func (t *ComputeTool) Renderer() *ChartRenderer { return t.renderer }
func (t *ComputeTool) Name() string             { return "compute" }
func (t *ComputeTool) ReadOnly() bool           { return true }
func (t *ComputeTool) Description() string {
	return "Execute pure-Go Starlark calculations with preloaded math, json, time, stats (mean/median/stdev/percentile/corr/linreg), finance (pmt/npv/irr/xirr/cagr/drawdown/dca/volatility/sharpe/sortino/sma/ema/rsi/macd/bollinger/bs_option), and chart (line/bar/pie/scatter/kline/compare PNG, plus chart.table(title, headers, rows, subtitle, highlight_col) and chart.card(title, sections, subtitle, footer) for travel itineraries & side-by-side comparisons)."
}

// ValidateStarlarkAST inspects the parsed AST before execution to block single-step & exponential OOM DoS
// such as `[0] * 100000000`, `n = 10**8; [0] * n`, `"a" * n`, `range(100000000)`, `x *= n`, or
// loop self-doubling `x = x + x` / `x += x`.
func ValidateStarlarkAST(src string) (*syntax.File, error) {
	opts := &syntax.FileOptions{
		Set:       true,
		While:     true,
		Recursion: false,
	}
	file, err := opts.Parse("compute.star", src, 0)
	if err != nil {
		return nil, fmt.Errorf("compute: syntax error: %w", err)
	}

	// Track known variable names holding sequence/string values (conservatively),
	// and variables produced by sequence repetition to prevent chained OOM like `a = [0]*10000; b = a*10000`.
	seqVars := make(map[string]bool)
	seqRepeatVars := make(map[string]int64)
	var blockErr error
	var walkStmt func(n syntax.Node, inLoop bool)
	walkStmt = func(n syntax.Node, inLoop bool) {
		if blockErr != nil || n == nil {
			return
		}
		syntax.Walk(n, func(cur syntax.Node) bool {
			if blockErr != nil || cur == nil {
				return false
			}
			switch x := cur.(type) {
			case *syntax.ForStmt:
				walkStmt(x.X, inLoop)
				for _, st := range x.Body {
					walkStmt(st, true)
				}
				return false
			case *syntax.WhileStmt:
				walkStmt(x.Cond, inLoop)
				for _, st := range x.Body {
					walkStmt(st, true)
				}
				return false
			case *syntax.AssignStmt:
				if lhsID, ok := x.LHS.(*syntax.Ident); ok {
					if isSequenceExpr(x.RHS, seqVars) {
						seqVars[lhsID.Name] = true
						if mult := estimateSeqMultiplier(x.RHS, seqVars, seqRepeatVars); mult > 1 {
							seqRepeatVars[lhsID.Name] = mult
						}
					}
					// Block exponential self-doubling inside loops: `x += x` or `x = x + x`.
					if inLoop {
						if x.Op == syntax.PLUS_EQ {
							if rhsID, ok2 := x.RHS.(*syntax.Ident); ok2 && rhsID.Name == lhsID.Name {
								blockErr = fmt.Errorf("%w: self-doubling %s += %s inside loop", ErrStarlarkOOMBlocked, lhsID.Name, rhsID.Name)
								return false
							}
						}
						if x.Op == syntax.EQ {
							if bin, ok2 := x.RHS.(*syntax.BinaryExpr); ok2 && bin.Op == syntax.PLUS {
								lx, okL := bin.X.(*syntax.Ident)
								ry, okR := bin.Y.(*syntax.Ident)
								if okL && okR && lx.Name == lhsID.Name && ry.Name == lhsID.Name {
									blockErr = fmt.Errorf("%w: self-doubling %s = %s + %s inside loop", ErrStarlarkOOMBlocked, lhsID.Name, lx.Name, ry.Name)
									return false
								}
							}
						}
					}
				}
				if x.Op == syntax.STAR_EQ {
					val, ok := evalConstInt(x.RHS)
					if !ok || val > maxStarlarkRepeatMul {
						// Allow numeric `*=` only if LHS is not a known sequence var AND not inside loop with huge multiplier.
						lhsID, isID := x.LHS.(*syntax.Ident)
						if (ok && val > maxStarlarkRepeatMul) || (isID && seqVars[lhsID.Name]) || inLoop {
							blockErr = fmt.Errorf("%w: unsafe augmented multiplication (*=)", ErrStarlarkOOMBlocked)
							return false
						}
					}
				}
			case *syntax.BinaryExpr:
				if x.Op == syntax.STAR {
					lhsSeq := isSequenceExpr(x.X, seqVars)
					rhsSeq := isSequenceExpr(x.Y, seqVars)
					if lhsSeq {
						val, ok := evalConstInt(x.Y)
						baseMul := estimateSeqMultiplier(x.X, seqVars, seqRepeatVars)
						if !ok || val > maxStarlarkRepeatMul || val < 0 || (baseMul > 1 && val > maxStarlarkRepeatMul/baseMul) {
							blockErr = fmt.Errorf("%w: sequence multiplication requires small constant <= %d", ErrStarlarkOOMBlocked, maxStarlarkRepeatMul)
							return false
						}
					}
					if rhsSeq {
						val, ok := evalConstInt(x.X)
						baseMul := estimateSeqMultiplier(x.Y, seqVars, seqRepeatVars)
						if !ok || val > maxStarlarkRepeatMul || val < 0 || (baseMul > 1 && val > maxStarlarkRepeatMul/baseMul) {
							blockErr = fmt.Errorf("%w: sequence multiplication requires small constant <= %d", ErrStarlarkOOMBlocked, maxStarlarkRepeatMul)
							return false
						}
					}
				}
			case *syntax.CallExpr:
				if fn, ok := x.Fn.(*syntax.Ident); ok && fn.Name == "range" {
					for _, arg := range x.Args {
						if val, ok := evalConstInt(arg); ok && absInt64(val) > maxStarlarkRangeLen {
							blockErr = fmt.Errorf("%w: range(%d) exceeds %d", ErrStarlarkOOMBlocked, val, maxStarlarkRangeLen)
							return false
						}
					}
				}
			}
			return true
		})
	}
	for _, st := range file.Stmts {
		walkStmt(st, false)
	}
	if blockErr != nil {
		return nil, blockErr
	}
	return file, nil
}

func isSequenceExpr(e syntax.Expr, seqVars map[string]bool) bool {
	switch x := e.(type) {
	case *syntax.ListExpr, *syntax.TupleExpr, *syntax.Comprehension:
		return true
	case *syntax.Literal:
		return x.Token == syntax.STRING
	case *syntax.Ident:
		return seqVars[x.Name]
	case *syntax.ParenExpr:
		return isSequenceExpr(x.X, seqVars)
	case *syntax.BinaryExpr:
		if x.Op == syntax.STAR || x.Op == syntax.PLUS {
			return isSequenceExpr(x.X, seqVars) || isSequenceExpr(x.Y, seqVars)
		}
	}
	return false
}

func estimateSeqMultiplier(e syntax.Expr, seqVars map[string]bool, seqRepeatVars map[string]int64) int64 {
	switch x := e.(type) {
	case *syntax.ParenExpr:
		return estimateSeqMultiplier(x.X, seqVars, seqRepeatVars)
	case *syntax.Ident:
		if m, ok := seqRepeatVars[x.Name]; ok && m > 1 {
			return m
		}
		return 1
	case *syntax.BinaryExpr:
		if x.Op == syntax.STAR {
			if isSequenceExpr(x.X, seqVars) {
				base := estimateSeqMultiplier(x.X, seqVars, seqRepeatVars)
				val, ok := evalConstInt(x.Y)
				if !ok || val <= 0 {
					return maxStarlarkRepeatMul + 1
				}
				if base > maxStarlarkRepeatMul/val {
					return maxStarlarkRepeatMul + 1
				}
				return base * val
			}
			if isSequenceExpr(x.Y, seqVars) {
				base := estimateSeqMultiplier(x.Y, seqVars, seqRepeatVars)
				val, ok := evalConstInt(x.X)
				if !ok || val <= 0 {
					return maxStarlarkRepeatMul + 1
				}
				if base > maxStarlarkRepeatMul/val {
					return maxStarlarkRepeatMul + 1
				}
				return base * val
			}
		}
		if x.Op == syntax.PLUS {
			lx := estimateSeqMultiplier(x.X, seqVars, seqRepeatVars)
			ly := estimateSeqMultiplier(x.Y, seqVars, seqRepeatVars)
			if lx+ly > maxStarlarkRepeatMul {
				return maxStarlarkRepeatMul + 1
			}
			return lx + ly
		}
	}
	return 1
}

func exceedsIntLiteral(e syntax.Expr, limit int64) bool {
	val, ok := evalConstInt(e)
	return ok && val > limit
}

func isSmallIntLiteral(e syntax.Expr, limit int64) bool {
	val, ok := evalConstInt(e)
	return ok && val <= limit
}

func evalConstInt(e syntax.Expr) (int64, bool) {
	switch x := e.(type) {
	case *syntax.Literal:
		if x.Token == syntax.INT {
			switch v := x.Value.(type) {
			case int64:
				return v, true
			}
		}
	case *syntax.ParenExpr:
		return evalConstInt(x.X)
	case *syntax.UnaryExpr:
		if v, ok := evalConstInt(x.X); ok {
			if x.Op == syntax.MINUS {
				return -v, true
			}
			return v, true
		}
	case *syntax.BinaryExpr:
		l, lok := evalConstInt(x.X)
		r, rok := evalConstInt(x.Y)
		if lok && rok {
			switch x.Op {
			case syntax.PLUS:
				return l + r, true
			case syntax.MINUS:
				return l - r, true
			case syntax.STAR:
				return l * r, true
			case syntax.LTLT:
				if r >= 0 && r < 60 {
					return l << uint(r), true
				}
				return math.MaxInt64, true
			}
		}
	}
	return 0, false
}

func absInt64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func (t *ComputeTool) Execute(ctx context.Context, _ ToolContext, args ComputeArgs) (ToolResult, error) {
	file, err := ValidateStarlarkAST(args.Code)
	if err != nil {
		return ToolResult{}, err
	}

	var outBuf strings.Builder
	var charts []model.DirectMedia

	thread := &starlark.Thread{
		Name: "fatfish-compute",
		Print: func(_ *starlark.Thread, msg string) {
			if outBuf.Len() < maxStarlarkOutputLen {
				outBuf.WriteString(msg)
				outBuf.WriteByte('\n')
			}
		},
	}
	thread.SetMaxExecutionSteps(t.maxSteps)

	// Enforce wall-clock timeout via thread.Cancel.
	runCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		select {
		case <-runCtx.Done():
			thread.Cancel("execution timeout exceeded")
		case <-done:
		}
	}()

	predeclared := starlark.StringDict{
		"math":    starmath.Module,
		"json":    starjson.Module,
		"time":    startime.Module,
		"stats":   statsModule(),
		"finance": financeModule(),
		"chart":   t.chartModule(&charts),
		"range":   starlark.NewBuiltin("range", slSafeRange),
		"round":   starlark.NewBuiltin("round", slRound),
		"sum":     starlark.NewBuiltin("sum", slSum),
	}

	prog, err := starlark.FileProgram(file, predeclared.Has)
	if err != nil {
		close(done)
		return ToolResult{}, fmt.Errorf("compute: compile error: %w", err)
	}

	globals, err := prog.Init(thread, predeclared)
	close(done)
	if err != nil {
		return ToolResult{}, fmt.Errorf("compute: runtime error: %w", err)
	}

	output := strings.TrimSpace(outBuf.String())
	if output == "" {
		if resVal, ok := globals["result"]; ok {
			output = resVal.String()
		} else if len(charts) > 0 {
			output = fmt.Sprintf("Generated %d chart image(s).", len(charts))
		} else {
			output = "(executed successfully, no printed output)"
		}
	}
	if len(output) > maxStarlarkOutputLen {
		output = output[:maxStarlarkOutputLen] + "\n...(truncated)"
	}
	return ToolResult{
		TextForLLM:  output,
		DirectMedia: charts,
	}, nil
}

// --- Safe runtime range wrapper & Pythonic builtins (round, sum) ---

// slSafeRange enforces maxStarlarkRangeLen at runtime even when range() is called with dynamic variables.
func slSafeRange(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var start, stop, step int64 = 0, 0, 1
	switch len(args) {
	case 1:
		if err := starlark.UnpackPositionalArgs("range", args, kwargs, 1, &stop); err != nil {
			return nil, err
		}
	case 2:
		if err := starlark.UnpackPositionalArgs("range", args, kwargs, 2, &start, &stop); err != nil {
			return nil, err
		}
	case 3:
		if err := starlark.UnpackPositionalArgs("range", args, kwargs, 3, &start, &stop, &step); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("range: got %d arguments, want 1..3", len(args))
	}
	if step == 0 {
		return nil, errors.New("range: step argument must not be zero")
	}
	var count int64
	if step > 0 && stop > start {
		count = (stop - start - 1)/step + 1
	} else if step < 0 && stop < start {
		count = (start - stop - 1)/(-step) + 1
	}
	if count > maxStarlarkRangeLen {
		return nil, fmt.Errorf("%w: runtime range length %d exceeds %d", ErrStarlarkOOMBlocked, count, maxStarlarkRangeLen)
	}
	return starlark.Universe["range"].(*starlark.Builtin).CallInternal(thread, args, kwargs)
}

func slRound(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var xVal starlark.Value
	ndigits := 0
	if err := starlark.UnpackPositionalArgs("round", args, kwargs, 1, &xVal, &ndigits); err != nil {
		return nil, err
	}
	x, err := toFloat(xVal)
	if err != nil {
		return nil, err
	}
	pow := math.Pow(10, float64(ndigits))
	return starlark.Float(math.Round(x*pow) / pow), nil
}

func slSum(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var seq starlark.Value
	var start float64
	if err := starlark.UnpackPositionalArgs("sum", args, kwargs, 1, &seq, &start); err != nil {
		return nil, err
	}
	xs, err := toFloatSlice(seq)
	if err != nil {
		return nil, err
	}
	total := start
	for _, x := range xs {
		total += x
	}
	return starlark.Float(total), nil
}

// --- Preloaded Go-native stats module ---

func statsModule() *starlarkstruct.Module {
	return &starlarkstruct.Module{
		Name: "stats",
		Members: starlark.StringDict{
			"mean":       starlark.NewBuiltin("stats.mean", slStatsMean),
			"median":     starlark.NewBuiltin("stats.median", slStatsMedian),
			"stdev":      starlark.NewBuiltin("stats.stdev", slStatsStdev),
			"percentile": starlark.NewBuiltin("stats.percentile", slStatsPercentile),
			"corr":       starlark.NewBuiltin("stats.corr", slStatsCorr),
			"linreg":     starlark.NewBuiltin("stats.linreg", slStatsLinreg),
		},
	}
}

func slStatsMean(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var seq starlark.Value
	if err := starlark.UnpackPositionalArgs("mean", args, kwargs, 1, &seq); err != nil {
		return nil, err
	}
	xs, err := toFloatSlice(seq)
	if err != nil || len(xs) == 0 {
		return nil, errors.New("stats.mean: non-empty numeric list required")
	}
	return starlark.Float(meanSlice(xs)), nil
}

func slStatsMedian(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var seq starlark.Value
	if err := starlark.UnpackPositionalArgs("median", args, kwargs, 1, &seq); err != nil {
		return nil, err
	}
	xs, err := toFloatSlice(seq)
	if err != nil || len(xs) == 0 {
		return nil, errors.New("stats.median: non-empty numeric list required")
	}
	return starlark.Float(percentileSlice(xs, 50)), nil
}

func slStatsStdev(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var seq starlark.Value
	if err := starlark.UnpackPositionalArgs("stdev", args, kwargs, 1, &seq); err != nil {
		return nil, err
	}
	xs, err := toFloatSlice(seq)
	if err != nil || len(xs) < 2 {
		return nil, errors.New("stats.stdev: >=2 numbers required")
	}
	m := meanSlice(xs)
	var sum float64
	for _, x := range xs {
		d := x - m
		sum += d * d
	}
	return starlark.Float(math.Sqrt(sum / float64(len(xs)-1))), nil
}

func slStatsPercentile(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var seq, pVal starlark.Value
	if err := starlark.UnpackPositionalArgs("percentile", args, kwargs, 2, &seq, &pVal); err != nil {
		return nil, err
	}
	p, err := toFloat(pVal)
	if err != nil {
		return nil, err
	}
	xs, err := toFloatSlice(seq)
	if err != nil || len(xs) == 0 {
		return nil, errors.New("stats.percentile: non-empty numeric list required")
	}
	return starlark.Float(percentileSlice(xs, p)), nil
}

func slStatsCorr(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var sx, sy starlark.Value
	if err := starlark.UnpackPositionalArgs("corr", args, kwargs, 2, &sx, &sy); err != nil {
		return nil, err
	}
	xs, err := toFloatSlice(sx)
	if err != nil {
		return nil, err
	}
	ys, err := toFloatSlice(sy)
	if err != nil || len(xs) != len(ys) || len(xs) < 2 {
		return nil, errors.New("stats.corr: equal-length numeric lists (>=2) required")
	}
	mx, my := meanSlice(xs), meanSlice(ys)
	var num, dx2, dy2 float64
	for i := range xs {
		dx, dy := xs[i]-mx, ys[i]-my
		num += dx * dy
		dx2 += dx * dx
		dy2 += dy * dy
	}
	den := math.Sqrt(dx2 * dy2)
	if den == 0 {
		return starlark.Float(0), nil
	}
	return starlark.Float(num / den), nil
}

func slStatsLinreg(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var sx, sy starlark.Value
	if err := starlark.UnpackPositionalArgs("linreg", args, kwargs, 2, &sx, &sy); err != nil {
		return nil, err
	}
	xs, err := toFloatSlice(sx)
	if err != nil {
		return nil, err
	}
	ys, err := toFloatSlice(sy)
	if err != nil || len(xs) != len(ys) || len(xs) < 2 {
		return nil, errors.New("stats.linreg: equal-length numeric lists (>=2) required")
	}
	mx, my := meanSlice(xs), meanSlice(ys)
	var cov, vx float64
	for i := range xs {
		dx := xs[i] - mx
		cov += dx * (ys[i] - my)
		vx += dx * dx
	}
	slope := 0.0
	if vx != 0 {
		slope = cov / vx
	}
	intercept := my - slope*mx
	d := starlark.NewDict(2)
	_ = d.SetKey(starlark.String("slope"), starlark.Float(slope))
	_ = d.SetKey(starlark.String("intercept"), starlark.Float(intercept))
	return d, nil
}

// --- Preloaded Go-native finance module ---

func financeModule() *starlarkstruct.Module {
	return &starlarkstruct.Module{
		Name: "finance",
		Members: starlark.StringDict{
			"pmt":        starlark.NewBuiltin("finance.pmt", slFinPMT),
			"npv":        starlark.NewBuiltin("finance.npv", slFinNPV),
			"irr":        starlark.NewBuiltin("finance.irr", slFinIRR),
			"xirr":       starlark.NewBuiltin("finance.xirr", slFinXIRR),
			"cagr":       starlark.NewBuiltin("finance.cagr", slFinCAGR),
			"drawdown":   starlark.NewBuiltin("finance.drawdown", slFinDrawdown),
			"dca":        starlark.NewBuiltin("finance.dca", slFinDCA),
			"volatility": starlark.NewBuiltin("finance.volatility", slFinVolatility),
			"sharpe":     starlark.NewBuiltin("finance.sharpe", slFinSharpe),
			"sortino":    starlark.NewBuiltin("finance.sortino", slFinSortino),
			"sma":        starlark.NewBuiltin("finance.sma", slFinSMA),
			"ema":        starlark.NewBuiltin("finance.ema", slFinEMA),
			"rsi":        starlark.NewBuiltin("finance.rsi", slFinRSI),
			"macd":       starlark.NewBuiltin("finance.macd", slFinMACD),
			"bollinger":  starlark.NewBuiltin("finance.bollinger", slFinBollinger),
			"bs_option":  starlark.NewBuiltin("finance.bs_option", slFinBSOption),
		},
	}
}

func slFinPMT(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var rVal, nVal, pvVal starlark.Value
	if err := starlark.UnpackPositionalArgs("pmt", args, kwargs, 3, &rVal, &nVal, &pvVal); err != nil {
		return nil, err
	}
	rate, err := toFloat(rVal)
	if err != nil {
		return nil, err
	}
	nper, err := toFloat(nVal)
	if err != nil {
		return nil, err
	}
	pv, err := toFloat(pvVal)
	if err != nil {
		return nil, err
	}
	if nper <= 0 {
		return nil, errors.New("finance.pmt: nper must be > 0")
	}
	if rate == 0 {
		return starlark.Float(-pv / nper), nil
	}
	f := math.Pow(1+rate, nper)
	return starlark.Float(-pv * rate * f / (f - 1)), nil
}

func slFinNPV(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var rVal, cfVal starlark.Value
	if err := starlark.UnpackPositionalArgs("npv", args, kwargs, 2, &rVal, &cfVal); err != nil {
		return nil, err
	}
	rate, err := toFloat(rVal)
	if err != nil {
		return nil, err
	}
	cfs, err := toFloatSlice(cfVal)
	if err != nil {
		return nil, err
	}
	var total float64
	for i, cf := range cfs {
		total += cf / math.Pow(1+rate, float64(i))
	}
	return starlark.Float(total), nil
}

func slFinIRR(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var cfVal starlark.Value
	if err := starlark.UnpackPositionalArgs("irr", args, kwargs, 1, &cfVal); err != nil {
		return nil, err
	}
	cfs, err := toFloatSlice(cfVal)
	if err != nil || len(cfs) < 2 {
		return nil, errors.New("finance.irr: >=2 cashflows required")
	}
	times := make([]float64, len(cfs))
	for i := range cfs {
		times[i] = float64(i)
	}
	r, err := solveRate(cfs, times)
	if err != nil {
		return nil, fmt.Errorf("finance.irr: %w", err)
	}
	return starlark.Float(r), nil
}

func slFinXIRR(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var cfVal, dtVal starlark.Value
	if err := starlark.UnpackPositionalArgs("xirr", args, kwargs, 2, &cfVal, &dtVal); err != nil {
		return nil, err
	}
	cfs, err := toFloatSlice(cfVal)
	if err != nil || len(cfs) < 2 {
		return nil, errors.New("finance.xirr: >=2 cashflows required")
	}
	dateStrs, err := toStringSlice(dtVal)
	if err != nil || len(dateStrs) != len(cfs) {
		return nil, errors.New("finance.xirr: dates list must match cashflows length (YYYY-MM-DD)")
	}
	dates := make([]time.Time, len(dateStrs))
	for i, s := range dateStrs {
		t, err := time.Parse("2006-01-02", strings.TrimSpace(s))
		if err != nil {
			return nil, fmt.Errorf("finance.xirr: invalid date %q (want YYYY-MM-DD)", s)
		}
		dates[i] = t
	}
	t0 := dates[0]
	years := make([]float64, len(dates))
	for i, d := range dates {
		years[i] = d.Sub(t0).Hours() / (24.0 * 365.0)
	}
	r, err := solveRate(cfs, years)
	if err != nil {
		return nil, fmt.Errorf("finance.xirr: %w", err)
	}
	return starlark.Float(r), nil
}

// solveRate solves sum(cf_i / (1+r)^t_i) = 0 using guarded Newton-Raphson with bisection fallback.
func solveRate(cfs, times []float64) (float64, error) {
	hasPos, hasNeg := false, false
	for _, cf := range cfs {
		if cf > 0 {
			hasPos = true
		} else if cf < 0 {
			hasNeg = true
		}
	}
	if !hasPos || !hasNeg {
		return 0, errors.New("cashflows must contain at least one positive and one negative value")
	}
	eval := func(r float64) (npv, dnpv float64) {
		for i, cf := range cfs {
			t := times[i]
			denom := math.Pow(1+r, t)
			if math.IsNaN(denom) || math.IsInf(denom, 0) || denom == 0 {
				return math.NaN(), math.NaN()
			}
			npv += cf / denom
			if t != 0 {
				dnpv -= t * cf / (denom * (1 + r))
			}
		}
		return npv, dnpv
	}

	// Try guarded Newton-Raphson from a few sensible initial guesses.
	for _, guess := range []float64{0.1, 0.0, 0.5, -0.3} {
		r := guess
		for iter := 0; iter < 80; iter++ {
			npv, dnpv := eval(r)
			if math.IsNaN(npv) || math.IsInf(npv, 0) {
				break
			}
			if math.Abs(npv) < 1e-8 {
				return r, nil
			}
			if math.IsNaN(dnpv) || math.Abs(dnpv) < 1e-12 {
				break
			}
			step := npv / dnpv
			if step > 1.0 {
				step = 1.0
			} else if step < -1.0 {
				step = -1.0
			}
			next := r - step
			if next <= -0.9999 {
				next = -0.999
			} else if next > 1000 {
				next = 1000
			}
			r = next
		}
	}

	// Bisection fallback across progressive brackets.
	grid := []float64{-0.99, -0.9, -0.7, -0.5, -0.2, 0.0, 0.1, 0.3, 0.8, 2.0, 5.0, 20.0, 100.0}
	for i := 0; i < len(grid)-1; i++ {
		lo, hi := grid[i], grid[i+1]
		fLo, _ := eval(lo)
		fHi, _ := eval(hi)
		if math.IsNaN(fLo) || math.IsNaN(fHi) {
			continue
		}
		if fLo == 0 {
			return lo, nil
		}
		if fHi == 0 {
			return hi, nil
		}
		if (fLo < 0 && fHi > 0) || (fLo > 0 && fHi < 0) {
			for iter := 0; iter < 120; iter++ {
				mid := 0.5 * (lo + hi)
				fMid, _ := eval(mid)
				if math.Abs(fMid) < 1e-8 || (hi-lo) < 1e-10 {
					return mid, nil
				}
				if (fLo < 0 && fMid < 0) || (fLo > 0 && fMid > 0) {
					lo, fLo = mid, fMid
				} else {
					hi = mid
				}
			}
			return 0.5 * (lo + hi), nil
		}
	}
	return 0, errors.New("failed to converge on a valid rate")
}

func slFinCAGR(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var sVal, eVal, yVal starlark.Value
	if err := starlark.UnpackPositionalArgs("cagr", args, kwargs, 3, &sVal, &eVal, &yVal); err != nil {
		return nil, err
	}
	start, err := toFloat(sVal)
	if err != nil {
		return nil, err
	}
	end, err := toFloat(eVal)
	if err != nil {
		return nil, err
	}
	years, err := toFloat(yVal)
	if err != nil {
		return nil, err
	}
	if start <= 0 || end <= 0 || years <= 0 {
		return nil, errors.New("finance.cagr: start, end, and years must be > 0")
	}
	return starlark.Float(math.Pow(end/start, 1/years) - 1), nil
}

func slFinDrawdown(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var seq starlark.Value
	if err := starlark.UnpackPositionalArgs("drawdown", args, kwargs, 1, &seq); err != nil {
		return nil, err
	}
	xs, err := toFloatSlice(seq)
	if err != nil || len(xs) == 0 {
		return nil, errors.New("finance.drawdown: non-empty price series required")
	}
	peak := xs[0]
	maxDD := 0.0
	for _, x := range xs {
		if x > peak {
			peak = x
		}
		if peak > 0 {
			dd := (peak - x) / peak
			if dd > maxDD {
				maxDD = dd
			}
		}
	}
	return starlark.Float(maxDD), nil
}

func slFinDCA(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var seq, amtVal starlark.Value
	if err := starlark.UnpackPositionalArgs("dca", args, kwargs, 2, &seq, &amtVal); err != nil {
		return nil, err
	}
	prices, err := toFloatSlice(seq)
	if err != nil || len(prices) == 0 {
		return nil, errors.New("finance.dca: non-empty price series required")
	}
	amount, err := toFloat(amtVal)
	if err != nil || amount <= 0 {
		return nil, errors.New("finance.dca: positive amount per period required")
	}
	var totalInvested, shares, peakVal, maxDD float64
	for _, p := range prices {
		if p <= 0 {
			return nil, errors.New("finance.dca: all prices must be > 0")
		}
		totalInvested += amount
		shares += amount / p
		val := shares * p
		if val > peakVal {
			peakVal = val
		}
		if peakVal > 0 {
			if dd := (peakVal - val) / peakVal; dd > maxDD {
				maxDD = dd
			}
		}
	}
	finalVal := shares * prices[len(prices)-1]
	avgCost := totalInvested / shares
	totalRet := (finalVal - totalInvested) / totalInvested
	d := starlark.NewDict(6)
	_ = d.SetKey(starlark.String("total_invested"), starlark.Float(totalInvested))
	_ = d.SetKey(starlark.String("shares"), starlark.Float(shares))
	_ = d.SetKey(starlark.String("final_value"), starlark.Float(finalVal))
	_ = d.SetKey(starlark.String("avg_cost"), starlark.Float(avgCost))
	_ = d.SetKey(starlark.String("total_return"), starlark.Float(totalRet))
	_ = d.SetKey(starlark.String("max_drawdown"), starlark.Float(maxDD))
	return d, nil
}

func slFinVolatility(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var seq starlark.Value
	var annVal starlark.Value = starlark.Float(252)
	if err := starlark.UnpackArgs("volatility", args, kwargs, "prices", &seq, "annualize?", &annVal); err != nil {
		return nil, err
	}
	prices, err := toFloatSlice(seq)
	if err != nil || len(prices) < 3 {
		return nil, errors.New("finance.volatility: >=3 prices required")
	}
	ann, err := toFloat(annVal)
	if err != nil || ann <= 0 {
		ann = 252
	}
	rets := make([]float64, 0, len(prices)-1)
	for i := 1; i < len(prices); i++ {
		if prices[i-1] <= 0 || prices[i] <= 0 {
			return nil, errors.New("finance.volatility: prices must be > 0")
		}
		rets = append(rets, math.Log(prices[i]/prices[i-1]))
	}
	return starlark.Float(sampleStdev(rets) * math.Sqrt(ann)), nil
}

func slFinSharpe(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var seq starlark.Value
	var rfVal starlark.Value = starlark.Float(0)
	var annVal starlark.Value = starlark.Float(252)
	if err := starlark.UnpackArgs("sharpe", args, kwargs, "returns", &seq, "rf?", &rfVal, "annualize?", &annVal); err != nil {
		return nil, err
	}
	rets, err := toFloatSlice(seq)
	if err != nil || len(rets) < 2 {
		return nil, errors.New("finance.sharpe: >=2 period returns required")
	}
	rf, _ := toFloat(rfVal)
	ann, _ := toFloat(annVal)
	if ann <= 0 {
		ann = 252
	}
	rfPer := rf / ann
	excess := make([]float64, len(rets))
	for i, r := range rets {
		excess[i] = r - rfPer
	}
	sd := sampleStdev(excess)
	if sd == 0 {
		return starlark.Float(0), nil
	}
	return starlark.Float(meanSlice(excess) / sd * math.Sqrt(ann)), nil
}

func slFinSortino(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var seq starlark.Value
	var rfVal starlark.Value = starlark.Float(0)
	var annVal starlark.Value = starlark.Float(252)
	if err := starlark.UnpackArgs("sortino", args, kwargs, "returns", &seq, "rf?", &rfVal, "annualize?", &annVal); err != nil {
		return nil, err
	}
	rets, err := toFloatSlice(seq)
	if err != nil || len(rets) < 2 {
		return nil, errors.New("finance.sortino: >=2 period returns required")
	}
	rf, _ := toFloat(rfVal)
	ann, _ := toFloat(annVal)
	if ann <= 0 {
		ann = 252
	}
	rfPer := rf / ann
	var sumEx, sumDown2 float64
	for _, r := range rets {
		ex := r - rfPer
		sumEx += ex
		if ex < 0 {
			sumDown2 += ex * ex
		}
	}
	downDev := math.Sqrt(sumDown2 / float64(len(rets)))
	if downDev == 0 {
		return starlark.Float(0), nil
	}
	return starlark.Float((sumEx / float64(len(rets))) / downDev * math.Sqrt(ann)), nil
}

func slFinSMA(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var seq starlark.Value
	var win int
	if err := starlark.UnpackPositionalArgs("sma", args, kwargs, 2, &seq, &win); err != nil {
		return nil, err
	}
	prices, err := toFloatSlice(seq)
	if err != nil || win <= 0 || len(prices) < win {
		return nil, errors.New("finance.sma: valid price list with len >= window required")
	}
	res := make([]starlark.Value, 0, len(prices)-win+1)
	sum := 0.0
	for i, p := range prices {
		sum += p
		if i >= win {
			sum -= prices[i-win]
		}
		if i+1 >= win {
			res = append(res, starlark.Float(sum/float64(win)))
		}
	}
	return starlark.NewList(res), nil
}

func slFinEMA(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var seq starlark.Value
	var win int
	if err := starlark.UnpackPositionalArgs("ema", args, kwargs, 2, &seq, &win); err != nil {
		return nil, err
	}
	prices, err := toFloatSlice(seq)
	if err != nil || win <= 0 || len(prices) == 0 {
		return nil, errors.New("finance.ema: non-empty price list and positive window required")
	}
	emas := calcEMASlice(prices, win)
	res := make([]starlark.Value, len(emas))
	for i, v := range emas {
		res[i] = starlark.Float(v)
	}
	return starlark.NewList(res), nil
}

func slFinRSI(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var seq starlark.Value
	period := 14
	if err := starlark.UnpackArgs("rsi", args, kwargs, "prices", &seq, "period?", &period); err != nil {
		return nil, err
	}
	prices, err := toFloatSlice(seq)
	if err != nil || period <= 0 || len(prices) <= period {
		return nil, errors.New("finance.rsi: len(prices) must be > period")
	}
	return starlark.Float(calcWilderRSI(prices, period)), nil
}

func slFinMACD(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var seq starlark.Value
	fast, slow, signal := 12, 26, 9
	if err := starlark.UnpackArgs("macd", args, kwargs, "prices", &seq, "fast?", &fast, "slow?", &slow, "signal?", &signal); err != nil {
		return nil, err
	}
	prices, err := toFloatSlice(seq)
	if err != nil || len(prices) < 2 || fast <= 0 || slow <= 0 || signal <= 0 {
		return nil, errors.New("finance.macd: >=2 prices and positive periods required")
	}
	eFast := calcEMASlice(prices, fast)
	eSlow := calcEMASlice(prices, slow)
	dif := make([]float64, len(prices))
	for i := range prices {
		dif[i] = eFast[i] - eSlow[i]
	}
	dea := calcEMASlice(dif, signal)
	hist := make([]float64, len(prices))
	for i := range prices {
		hist[i] = 2 * (dif[i] - dea[i])
	}
	last := len(prices) - 1
	d := starlark.NewDict(3)
	_ = d.SetKey(starlark.String("dif"), starlark.Float(dif[last]))
	_ = d.SetKey(starlark.String("dea"), starlark.Float(dea[last]))
	_ = d.SetKey(starlark.String("hist"), starlark.Float(hist[last]))
	return d, nil
}

func slFinBollinger(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var seq starlark.Value
	window := 20
	var kVal starlark.Value = starlark.Float(2.0)
	if err := starlark.UnpackArgs("bollinger", args, kwargs, "prices", &seq, "window?", &window, "k?", &kVal); err != nil {
		return nil, err
	}
	prices, err := toFloatSlice(seq)
	if err != nil || window < 2 || len(prices) < window {
		return nil, errors.New("finance.bollinger: len(prices) must be >= window (>=2)")
	}
	k, _ := toFloat(kVal)
	if k <= 0 {
		k = 2.0
	}
	tail := prices[len(prices)-window:]
	mid := meanSlice(tail)
	var sum2 float64
	for _, x := range tail {
		d := x - mid
		sum2 += d * d
	}
	sd := math.Sqrt(sum2 / float64(window))
	upper := mid + k*sd
	lower := mid - k*sd
	bw := 0.0
	if mid != 0 {
		bw = (upper - lower) / mid
	}
	d := starlark.NewDict(4)
	_ = d.SetKey(starlark.String("upper"), starlark.Float(upper))
	_ = d.SetKey(starlark.String("mid"), starlark.Float(mid))
	_ = d.SetKey(starlark.String("lower"), starlark.Float(lower))
	_ = d.SetKey(starlark.String("bandwidth"), starlark.Float(bw))
	return d, nil
}

func slFinBSOption(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var sVal, kVal, tVal, rVal, sigVal starlark.Value
	kind := "call"
	if err := starlark.UnpackArgs("bs_option", args, kwargs,
		"S", &sVal, "K", &kVal, "T", &tVal, "r", &rVal, "sigma", &sigVal, "kind?", &kind); err != nil {
		return nil, err
	}
	S, err1 := toFloat(sVal)
	K, err2 := toFloat(kVal)
	T, err3 := toFloat(tVal)
	r, err4 := toFloat(rVal)
	sigma, err5 := toFloat(sigVal)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil {
		return nil, errors.New("finance.bs_option: S, K, T, r, sigma must be numeric")
	}
	if S <= 0 || K <= 0 || T <= 0 || sigma <= 0 {
		return nil, errors.New("finance.bs_option: S, K, T, sigma must be > 0")
	}
	sqrtT := math.Sqrt(T)
	d1 := (math.Log(S/K) + (r+0.5*sigma*sigma)*T) / (sigma * sqrtT)
	d2 := d1 - sigma*sqrtT
	nd1 := stdNormCDF(d1)
	nd2 := stdNormCDF(d2)
	pdf1 := stdNormPDF(d1)
	disc := math.Exp(-r * T)

	var price, delta, theta, rho float64
	if strings.ToLower(strings.TrimSpace(kind)) == "put" {
		price = K*disc*stdNormCDF(-d2) - S*stdNormCDF(-d1)
		delta = nd1 - 1.0
		theta = (-S*pdf1*sigma/(2*sqrtT) + r*K*disc*stdNormCDF(-d2)) / 365.0
		rho = -K * T * disc * stdNormCDF(-d2) / 100.0
	} else {
		price = S*nd1 - K*disc*nd2
		delta = nd1
		theta = (-S*pdf1*sigma/(2*sqrtT) - r*K*disc*nd2) / 365.0
		rho = K * T * disc * nd2 / 100.0
	}
	gamma := pdf1 / (S * sigma * sqrtT)
	vega := S * pdf1 * sqrtT / 100.0

	d := starlark.NewDict(6)
	_ = d.SetKey(starlark.String("price"), starlark.Float(price))
	_ = d.SetKey(starlark.String("delta"), starlark.Float(delta))
	_ = d.SetKey(starlark.String("gamma"), starlark.Float(gamma))
	_ = d.SetKey(starlark.String("theta"), starlark.Float(theta))
	_ = d.SetKey(starlark.String("vega"), starlark.Float(vega))
	_ = d.SetKey(starlark.String("rho"), starlark.Float(rho))
	return d, nil
}

func calcEMASlice(xs []float64, window int) []float64 {
	out := make([]float64, len(xs))
	k := 2.0 / float64(window+1)
	out[0] = xs[0]
	for i := 1; i < len(xs); i++ {
		out[i] = xs[i]*k + out[i-1]*(1-k)
	}
	return out
}

func calcWilderRSI(prices []float64, period int) float64 {
	if len(prices) <= period || period <= 0 {
		return math.NaN()
	}
	var avgGain, avgLoss float64
	for i := 1; i <= period; i++ {
		diff := prices[i] - prices[i-1]
		if diff > 0 {
			avgGain += diff
		} else {
			avgLoss -= diff
		}
	}
	avgGain /= float64(period)
	avgLoss /= float64(period)
	for i := period + 1; i < len(prices); i++ {
		diff := prices[i] - prices[i-1]
		gain, loss := 0.0, 0.0
		if diff > 0 {
			gain = diff
		} else {
			loss = -diff
		}
		avgGain = (avgGain*float64(period-1) + gain) / float64(period)
		avgLoss = (avgLoss*float64(period-1) + loss) / float64(period)
	}
	if avgLoss == 0 {
		return 100.0
	}
	rs := avgGain / avgLoss
	return 100.0 - 100.0/(1.0+rs)
}

func sampleStdev(xs []float64) float64 {
	if len(xs) < 2 {
		return 0
	}
	m := meanSlice(xs)
	var sum float64
	for _, x := range xs {
		d := x - m
		sum += d * d
	}
	return math.Sqrt(sum / float64(len(xs)-1))
}

func stdNormCDF(x float64) float64 {
	return 0.5 * math.Erfc(-x/math.Sqrt2)
}

func stdNormPDF(x float64) float64 {
	return math.Exp(-0.5*x*x) / math.Sqrt(2*math.Pi)
}

// --- Preloaded Go-native chart module ---

func (t *ComputeTool) chartModule(charts *[]model.DirectMedia) *starlarkstruct.Module {
	checkCap := func() error {
		if len(*charts) >= maxChartsPerCompute {
			return fmt.Errorf("chart: maximum of %d charts per compute call exceeded", maxChartsPerCompute)
		}
		return nil
	}
	makeSeriesFn := func(kind string) func(*starlark.Thread, *starlark.Builtin, starlark.Tuple, []starlark.Tuple) (starlark.Value, error) {
		return func(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
			if err := checkCap(); err != nil {
				return nil, err
			}
			var title string
			var labelsVal, dataVal starlark.Value
			if err := starlark.UnpackArgs("chart."+kind, args, kwargs, "title", &title, "labels", &labelsVal, "data", &dataVal); err != nil {
				return nil, err
			}
			labels, _ := toStringSlice(labelsVal)
			series, err := toSeriesSlice(dataVal)
			if err != nil {
				return nil, err
			}
			pngBytes, err := t.renderer.RenderLineOrBar(kind, title, labels, series)
			if err != nil {
				return nil, err
			}
			*charts = append(*charts, model.DirectMedia{
				Data:     pngBytes,
				MimeType: "image/png",
				Filename: kind + ".png",
				Caption:  title,
			})
			return starlark.String(fmt.Sprintf("rendered %s chart (%d bytes)", kind, len(pngBytes))), nil
		}
	}

	pieFn := func(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if err := checkCap(); err != nil {
			return nil, err
		}
		var title string
		var labelsVal, valsVal starlark.Value
		if err := starlark.UnpackArgs("chart.pie", args, kwargs, "title", &title, "labels", &labelsVal, "values", &valsVal); err != nil {
			return nil, err
		}
		labels, _ := toStringSlice(labelsVal)
		vals, err := toFloatSlice(valsVal)
		if err != nil {
			return nil, err
		}
		pngBytes, err := t.renderer.RenderPie(title, labels, vals)
		if err != nil {
			return nil, err
		}
		*charts = append(*charts, model.DirectMedia{Data: pngBytes, MimeType: "image/png", Filename: "pie.png", Caption: title})
		return starlark.String(fmt.Sprintf("rendered pie chart (%d bytes)", len(pngBytes))), nil
	}

	klineFn := func(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if err := checkCap(); err != nil {
			return nil, err
		}
		var title string
		var candlesVal starlark.Value
		if err := starlark.UnpackArgs("chart.kline", args, kwargs, "title", &title, "candles", &candlesVal); err != nil {
			return nil, err
		}
		iter, ok := candlesVal.(starlark.Iterable)
		if !ok {
			return nil, errors.New("chart.kline: candles must be a list of [label, open, high, low, close]")
		}
		it := iter.Iterate()
		defer it.Done()
		var v starlark.Value
		var candles []Candle
		for it.Next(&v) {
			idx, ok := v.(starlark.Indexable)
			if !ok || idx.Len() < 5 {
				continue
			}
			lbl := strings.Trim(idx.Index(0).String(), `"`)
			o, _ := toFloat(idx.Index(1))
			h, _ := toFloat(idx.Index(2))
			l, _ := toFloat(idx.Index(3))
			c, _ := toFloat(idx.Index(4))
			vol := 0.0
			if idx.Len() >= 6 {
				vol, _ = toFloat(idx.Index(5))
			}
			candles = append(candles, Candle{Label: lbl, Open: o, High: h, Low: l, Close: c, Volume: vol})
		}
		pngBytes, err := t.renderer.RenderKline(title, candles)
		if err != nil {
			return nil, err
		}
		*charts = append(*charts, model.DirectMedia{Data: pngBytes, MimeType: "image/png", Filename: "kline.png", Caption: title})
		return starlark.String(fmt.Sprintf("rendered kline chart (%d bytes)", len(pngBytes))), nil
	}

	compareFn := func(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if err := checkCap(); err != nil {
			return nil, err
		}
		var title string
		var labelsVal, dataVal starlark.Value
		if err := starlark.UnpackArgs("chart.compare", args, kwargs, "title", &title, "labels", &labelsVal, "data", &dataVal); err != nil {
			return nil, err
		}
		labels, _ := toStringSlice(labelsVal)
		series, err := toSeriesSlice(dataVal)
		if err != nil {
			return nil, err
		}
		pngBytes, err := t.renderer.RenderCompare(title, labels, series)
		if err != nil {
			return nil, err
		}
		*charts = append(*charts, model.DirectMedia{Data: pngBytes, MimeType: "image/png", Filename: "compare.png", Caption: title})
		return starlark.String(fmt.Sprintf("rendered compare chart (%d bytes)", len(pngBytes))), nil
	}

	tableFn := func(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if err := checkCap(); err != nil {
			return nil, err
		}
		var title, subtitle string
		var headersVal, rowsVal starlark.Value
		highlightCol := -1
		if err := starlark.UnpackArgs("chart.table", args, kwargs,
			"title", &title,
			"headers", &headersVal,
			"rows", &rowsVal,
			"subtitle?", &subtitle,
			"highlight_col?", &highlightCol,
		); err != nil {
			return nil, err
		}
		headers, _ := toStringSlice(headersVal)
		rows, err := toStringMatrix(rowsVal)
		if err != nil {
			return nil, err
		}
		pngBytes, err := t.renderer.RenderTable(title, subtitle, headers, rows, highlightCol)
		if err != nil {
			return nil, err
		}
		*charts = append(*charts, model.DirectMedia{Data: pngBytes, MimeType: "image/png", Filename: "table.png", Caption: title})
		return starlark.String(fmt.Sprintf("rendered comparison table (%d bytes)", len(pngBytes))), nil
	}

	cardFn := func(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if err := checkCap(); err != nil {
			return nil, err
		}
		var title, subtitle, footer string
		var sectionsVal starlark.Value
		if err := starlark.UnpackArgs("chart.card", args, kwargs,
			"title", &title,
			"sections", &sectionsVal,
			"subtitle?", &subtitle,
			"footer?", &footer,
		); err != nil {
			return nil, err
		}
		sections, err := toCardSections(sectionsVal)
		if err != nil {
			return nil, err
		}
		pngBytes, err := t.renderer.RenderCard(title, subtitle, sections, footer)
		if err != nil {
			return nil, err
		}
		*charts = append(*charts, model.DirectMedia{Data: pngBytes, MimeType: "image/png", Filename: "card.png", Caption: title})
		return starlark.String(fmt.Sprintf("rendered guide card (%d bytes)", len(pngBytes))), nil
	}

	return &starlarkstruct.Module{
		Name: "chart",
		Members: starlark.StringDict{
			"line":    starlark.NewBuiltin("chart.line", makeSeriesFn("line")),
			"bar":     starlark.NewBuiltin("chart.bar", makeSeriesFn("bar")),
			"scatter": starlark.NewBuiltin("chart.scatter", makeSeriesFn("scatter")),
			"pie":     starlark.NewBuiltin("chart.pie", pieFn),
			"kline":   starlark.NewBuiltin("chart.kline", klineFn),
			"compare": starlark.NewBuiltin("chart.compare", compareFn),
			"table":   starlark.NewBuiltin("chart.table", tableFn),
			"card":    starlark.NewBuiltin("chart.card", cardFn),
		},
	}
}

func toStringMatrix(v starlark.Value) ([][]string, error) {
	iter, ok := v.(starlark.Iterable)
	if !ok {
		return nil, fmt.Errorf("chart.table: rows must be a list of lists, got %s", v.Type())
	}
	it := iter.Iterate()
	defer it.Done()
	var item starlark.Value
	var out [][]string
	for it.Next(&item) {
		row, _ := toStringSlice(item)
		out = append(out, row)
	}
	return out, nil
}

func toCardSections(v starlark.Value) ([]CardSection, error) {
	iter, ok := v.(starlark.Iterable)
	if !ok {
		return nil, fmt.Errorf("chart.card: sections must be a list of {heading, items} dicts, got %s", v.Type())
	}
	it := iter.Iterate()
	defer it.Done()
	var item starlark.Value
	var out []CardSection
	for it.Next(&item) {
		d, ok := item.(*starlark.Dict)
		if !ok {
			continue
		}
		var sec CardSection
		if hv, found, _ := d.Get(starlark.String("heading")); found {
			if hs, ok := hv.(starlark.String); ok {
				sec.Heading = string(hs)
			} else {
				sec.Heading = hv.String()
			}
		} else if hv, found, _ := d.Get(starlark.String("title")); found {
			if hs, ok := hv.(starlark.String); ok {
				sec.Heading = string(hs)
			} else {
				sec.Heading = hv.String()
			}
		}
		if iv, found, _ := d.Get(starlark.String("items")); found {
			sec.Items, _ = toStringSlice(iv)
		}
		out = append(out, sec)
	}
	return out, nil
}

func toFloat(v starlark.Value) (float64, error) {
	switch x := v.(type) {
	case starlark.Float:
		return float64(x), nil
	case starlark.Int:
		f := x.Float()
		return float64(f), nil
	default:
		return 0, fmt.Errorf("expected number, got %s", v.Type())
	}
}

func toFloatSlice(v starlark.Value) ([]float64, error) {
	iter, ok := v.(starlark.Iterable)
	if !ok {
		return nil, fmt.Errorf("expected iterable, got %s", v.Type())
	}
	it := iter.Iterate()
	defer it.Done()
	var item starlark.Value
	var out []float64
	for it.Next(&item) {
		f, err := toFloat(item)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, nil
}

func toStringSlice(v starlark.Value) ([]string, error) {
	iter, ok := v.(starlark.Iterable)
	if !ok {
		return nil, nil
	}
	it := iter.Iterate()
	defer it.Done()
	var item starlark.Value
	var out []string
	for it.Next(&item) {
		if s, ok := item.(starlark.String); ok {
			out = append(out, string(s))
		} else {
			out = append(out, item.String())
		}
	}
	return out, nil
}

func toSeriesSlice(v starlark.Value) ([]Series, error) {
	if d, ok := v.(*starlark.Dict); ok {
		keys := d.Keys()
		out := make([]Series, 0, len(keys))
		for _, k := range keys {
			val, _, _ := d.Get(k)
			nums, err := toFloatSlice(val)
			if err != nil {
				return nil, err
			}
			name := k.String()
			if ks, ok := k.(starlark.String); ok {
				name = string(ks)
			}
			out = append(out, Series{Name: name, Values: nums})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
		return out, nil
	}
	nums, err := toFloatSlice(v)
	if err != nil {
		return nil, err
	}
	return []Series{{Name: "Value", Values: nums}}, nil
}

func meanSlice(xs []float64) float64 {
	var s float64
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

func percentileSlice(xs []float64, p float64) float64 {
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	if p <= 0 {
		return cp[0]
	}
	if p >= 100 {
		return cp[len(cp)-1]
	}
	pos := (p / 100.0) * float64(len(cp)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return cp[lo]
	}
	return cp[lo] + (cp[hi]-cp[lo])*(pos-float64(lo))
}
