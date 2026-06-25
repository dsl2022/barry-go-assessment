// Command workflowdemo runs an "order pipeline" workflow that exercises every
// Task 2 feature: parallel branches, per-job retry with backoff, conditional
// routing, a sub-workflow step, all three concrete job types, and a lifecycle
// event bus with logging + metrics subscribers.
//
//	go run ./cmd/workflowdemo
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	wf "github.com/2015rpro/fantasy-assessment/task2-workflow"
)

// demoDoer is a stand-in HTTP client (no real network in the demo).
type demoDoer struct{}

func (demoDoer) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
}

// printMailer "sends" by printing.
type printMailer struct{}

func (printMailer) Send(ctx context.Context, to, subject, body string) error {
	fmt.Printf("    📧 email -> %s: %q\n", to, subject)
	return nil
}

func main() {
	// Event bus with two decoupled subscribers.
	metrics := wf.NewMetricsSubscriber()
	bus := wf.NewEventBus(
		wf.NewLoggingSubscriber(os.Stdout),
		metrics,
	)
	engine := wf.NewEngine(wf.WithEventBus(bus))

	// chargeCard is flaky: it fails on the first attempt, then succeeds — to
	// show retry-with-backoff driving a job to success.
	attempts := 0
	chargeCard := wf.JobFunc(func(ctx context.Context, ec *wf.ExecutionContext) (any, error) {
		attempts++
		if attempts == 1 {
			return nil, fmt.Errorf("payment gateway timeout")
		}
		return map[string]any{"charged": true}, nil
	})

	// Sub-workflow: a tiny audit workflow embedded as one step.
	audit := wf.NewWorkflow("audit").
		Add("write-audit", wf.JobFunc(func(ctx context.Context, ec *wf.ExecutionContext) (any, error) {
			fmt.Println("    🧾 audit record written")
			return "audited", nil
		}))

	order := wf.NewWorkflow("order-pipeline").
		// fetch the user (HTTP job)
		Add("fetchUser", &wf.HTTPCallJob{Client: demoDoer{}, Method: "GET", URL: "http://api/users/1"}).
		// two independent branches off fetchUser run in parallel:
		Add("chargeCard", chargeCard, wf.DependsOn("fetchUser"),
			wf.WithRetry(wf.RetryPolicy{MaxAttempts: 3, BaseDelay: 20 * time.Millisecond, Jitter: true})).
		Add("updateInventory", &wf.TransformJob{
			Source: "fetchUser",
			Fn:     func(in any) (any, error) { return "inventory-decremented", nil },
		}, wf.DependsOn("fetchUser")).
		// conditional routing: receipt on success, alert on failure (alert won't run)
		Add("sendReceipt", &wf.EmailSendJob{Mailer: printMailer{}, To: "buyer@x.com", Subject: "Your receipt"},
			wf.DependsOn("chargeCard"), wf.When(wf.OnState("chargeCard", wf.JobSucceeded))).
		Add("sendAlert", &wf.EmailSendJob{Mailer: printMailer{}, To: "oncall@x.com", Subject: "Charge failed"},
			wf.DependsOn("chargeCard"), wf.When(wf.OnState("chargeCard", wf.JobFailed))).
		// sub-workflow step after the receipt
		Add("audit", &wf.SubWorkflowJob{Engine: engine, Workflow: audit}, wf.DependsOn("sendReceipt"))

	fmt.Println("=== event stream ===")
	res, err := engine.Run(context.Background(), order)
	if err != nil {
		fmt.Println("run error:", err)
		os.Exit(1)
	}

	fmt.Println("\n=== final job states ===")
	for _, id := range []string{"fetchUser", "chargeCard", "updateInventory", "sendReceipt", "sendAlert", "audit"} {
		r := res.Results[id]
		fmt.Printf("  %-16s %-10s attempts=%d\n", id, r.State, r.Attempts)
	}

	fmt.Printf("\nworkflow result: %s\n", res.State)
	fmt.Printf("metrics: started=%d succeeded=%d failed=%d retrying=%d skipped=%d\n",
		metrics.Count(wf.EventJobStarted),
		metrics.Count(wf.EventJobSucceeded),
		metrics.Count(wf.EventJobFailed),
		metrics.Count(wf.EventJobRetrying),
		metrics.Count(wf.EventJobSkipped),
	)
}
