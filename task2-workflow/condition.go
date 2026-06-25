package workflow

import "fmt"

// Condition gates whether a node runs once its dependencies are complete. It's a
// small interface (not a bare func) so a condition can also describe itself for
// logs/events and be constructed from JSON config.
//
// Semantics in the engine:
//   - A node with NO explicit condition runs iff all its dependencies Succeeded
//     (the safe default — don't run on a broken upstream).
//   - A node WITH a condition runs iff the condition evaluates true; otherwise
//     it is Skipped (not Failed). This is what enables branches like
//     "run the alerting job only if the deploy job failed".
type Condition interface {
	Eval(ec *ExecutionContext) bool
	String() string
}

// ConditionFunc adapts a function (with a description) to a Condition.
type ConditionFunc struct {
	Desc string
	Fn   func(ec *ExecutionContext) bool
}

func (c ConditionFunc) Eval(ec *ExecutionContext) bool { return c.Fn(ec) }
func (c ConditionFunc) String() string                 { return c.Desc }

// OnState returns a condition that holds when the named job ended in want.
func OnState(jobID string, want JobState) Condition {
	return stateCondition{jobID: jobID, want: want}
}

type stateCondition struct {
	jobID string
	want  JobState
}

func (c stateCondition) Eval(ec *ExecutionContext) bool {
	r, ok := ec.Result(c.jobID)
	return ok && r.State == c.want
}
func (c stateCondition) String() string {
	return fmt.Sprintf("state(%s)==%s", c.jobID, c.want)
}

// OnOutputEquals returns a condition that holds when the named job's output
// equals want (compared with ==; values must be comparable).
func OnOutputEquals(jobID string, want any) Condition {
	return outputEqualsCondition{jobID: jobID, want: want}
}

type outputEqualsCondition struct {
	jobID string
	want  any
}

func (c outputEqualsCondition) Eval(ec *ExecutionContext) bool {
	out, ok := ec.Output(c.jobID)
	return ok && out == c.want
}
func (c outputEqualsCondition) String() string {
	return fmt.Sprintf("output(%s)==%v", c.jobID, c.want)
}
