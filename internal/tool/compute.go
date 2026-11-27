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
	Code string `json:"code" jsonschema:"required,description=Starlark script to execute. Preloaded modules: math, json, time, stats, finance, chart. Use print() for output."`
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
	return "Execute pure-Go Starlark calculations with preloaded math, json, time, stats (mean/median/stdev/percentile/corr/linreg), finance (pmt/irr/npv/cagr/drawdown), and chart (line/bar/pie/scatter/kline PNG)."
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
			"pmt":      starlark.NewBuiltin("finance.pmt", slFinPMT),
			"npv":      starlark.NewBuiltin("finance.npv", slFinNPV),
			"irr":      starlark.NewBuiltin("finance.irr", slFinIRR),
			"cagr":     starlark.NewBuiltin("finance.cagr", slFinCAGR),
			"drawdown": starlark.NewBuiltin("finance.drawdown", slFinDrawdown),
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
	r := 0.1
	for iter := 0; iter < 100; iter++ {
		var npv, dnpv float64
		for i, cf := range cfs {
			t := float64(i)
			denom := math.Pow(1+r, t)
			npv += cf / denom
			if i > 0 {
				dnpv -= t * cf / (denom * (1 + r))
			}
		}
		if math.Abs(npv) < 1e-9 || dnpv == 0 {
			break
		}
		r -= npv / dnpv
	}
	return starlark.Float(r), nil
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
			candles = append(candles, Candle{Label: lbl, Open: o, High: h, Low: l, Close: c})
		}
		pngBytes, err := t.renderer.RenderKline(title, candles)
		if err != nil {
			return nil, err
		}
		*charts = append(*charts, model.DirectMedia{Data: pngBytes, MimeType: "image/png", Filename: "kline.png", Caption: title})
		return starlark.String(fmt.Sprintf("rendered kline chart (%d bytes)", len(pngBytes))), nil
	}

	return &starlarkstruct.Module{
		Name: "chart",
		Members: starlark.StringDict{
			"line":    starlark.NewBuiltin("chart.line", makeSeriesFn("line")),
			"bar":     starlark.NewBuiltin("chart.bar", makeSeriesFn("bar")),
			"scatter": starlark.NewBuiltin("chart.scatter", makeSeriesFn("scatter")),
			"pie":     starlark.NewBuiltin("chart.pie", pieFn),
			"kline":   starlark.NewBuiltin("chart.kline", klineFn),
		},
	}
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
