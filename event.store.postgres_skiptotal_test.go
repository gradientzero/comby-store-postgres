package store_test

import (
	"context"
	"fmt"
	"testing"

	store "github.com/gradientzero/comby-store-postgres"
	"github.com/gradientzero/comby/v3"
)

// TestEventStoreListSkipTotal pins EventStoreListOptionSkipTotal: the rows are
// unaffected, and the total comes back as -1 ("not computed") instead of costing
// a second query.
//
// That second query counts over the SAME predicate as the one fetching the rows,
// so its cost follows how many rows MATCH rather than how many are returned. On
// a paged walk — which is what a read-model restore is — it is charged again for
// every page, and summed over the walk that is quadratic in the table while the
// useful work is linear.
//
// The assertion that matters here is the -1, not the speed: comby's in-memory
// store also answers -1 for a skipped total even though counting costs it
// nothing, precisely so that memory and SQL cannot drift apart unnoticed. A
// store that answered with a real number would pass every memory-backed test and
// differ only in production.
func TestEventStoreListSkipTotal(t *testing.T) {
	ctx := context.Background()

	eventStore := store.NewEventStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")
	if err := eventStore.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := eventStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	type TestDomainData struct {
		Name string
	}

	const aggregateUuid = "AggregateUuid_SkipTotal"
	const numEvents = 7
	for v := 1; v <= numEvents; v++ {
		evt := &comby.BaseEvent{
			InstanceId:    1,
			EventUuid:     comby.NewUuid(),
			TenantUuid:    "TenantUuid_1",
			CommandUuid:   comby.NewUuid(),
			AggregateUuid: aggregateUuid,
			Domain:        "Domain_1",
			CreatedAt:     int64(1000 + v),
			Version:       int64(v),
			DomainEvt:     &TestDomainData{Name: fmt.Sprintf("v%d", v)},
		}
		if err := eventStore.Create(ctx, comby.EventStoreCreateOptionWithEvent(evt)); err != nil {
			t.Fatalf("create version %d: %v", v, err)
		}
	}

	list := func(skip bool) ([]comby.Event, int64) {
		t.Helper()
		opts := []comby.EventStoreListOption{
			comby.EventStoreListOptionWithAggregateUuid(aggregateUuid),
			comby.EventStoreListOptionOrderBy("version"),
			comby.EventStoreListOptionAscending(true),
			comby.EventStoreListOptionOffset(0),
			comby.EventStoreListOptionLimit(3),
		}
		if skip {
			opts = append(opts, comby.EventStoreListOptionSkipTotal())
		}
		evts, total, err := eventStore.List(ctx, opts...)
		if err != nil {
			t.Fatalf("list(skip=%v): %v", skip, err)
		}
		return evts, total
	}

	// Unset: unchanged behaviour — a page of 3, and the full match count.
	evts, total := list(false)
	if len(evts) != 3 {
		t.Fatalf("without the option: %d rows, want 3", len(evts))
	}
	if total != numEvents {
		t.Fatalf("without the option: total = %d, want %d", total, numEvents)
	}

	// Set: the SAME page, and -1 for the total.
	skippedEvts, skippedTotal := list(true)
	if len(skippedEvts) != len(evts) {
		t.Fatalf("with the option: %d rows, want the same %d — skipping the total must not change what is returned",
			len(skippedEvts), len(evts))
	}
	for i := range evts {
		if skippedEvts[i].GetVersion() != evts[i].GetVersion() {
			t.Fatalf("with the option: row %d is version %d, want %d",
				i, skippedEvts[i].GetVersion(), evts[i].GetVersion())
		}
	}
	if skippedTotal != -1 {
		t.Fatalf("with the option: total = %d, want -1 (not computed)", skippedTotal)
	}
}
