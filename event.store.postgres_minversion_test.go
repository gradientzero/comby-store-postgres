package store_test

import (
	"context"
	"fmt"
	"testing"

	store "github.com/gradientzero/comby-store-postgres"
	"github.com/gradientzero/comby/v3"
)

// TestEventStoreListMinVersion pins EventStoreListOptionWithMinVersion, which
// this store used to accept and silently ignore.
//
// It is a quiet failure by construction: the in-memory event store DOES
// implement the filter (comby store.event.memory.go), so every test that runs
// against memory passes while the same code against Postgres returns the whole
// stream. The consequence is not a slow query, it is a wrong aggregate —
// AggregateRepository.GetAggregate restores from a snapshot at version N and
// then asks for the events after N, so an ignored filter replays everything
// from version 1 on top of the snapshot state and applies each of those events
// twice.
func TestEventStoreListMinVersion(t *testing.T) {
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

	const aggregateUuid = "AggregateUuid_MinVersion"
	const numVersions = 5
	for v := 1; v <= numVersions; v++ {
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

	list := func(minVersion int64) []int64 {
		t.Helper()
		evts, _, err := eventStore.List(ctx,
			comby.EventStoreListOptionWithAggregateUuid(aggregateUuid),
			comby.EventStoreListOptionWithMinVersion(minVersion),
			comby.EventStoreListOptionOrderBy("version"),
			comby.EventStoreListOptionAscending(true),
			comby.EventStoreListOptionOffset(0),
			comby.EventStoreListOptionLimit(100),
		)
		if err != nil {
			t.Fatalf("list minVersion=%d: %v", minVersion, err)
		}
		var versions []int64
		for _, e := range evts {
			versions = append(versions, e.GetVersion())
		}
		return versions
	}

	// 0 means no filter — the whole stream, unchanged behaviour.
	if got := list(0); len(got) != numVersions {
		t.Fatalf("minVersion=0 returned %v, want all %d versions", got, numVersions)
	}

	// Strictly greater than: 3 excludes 3.
	got := list(3)
	want := []int64{4, 5}
	if len(got) != len(want) {
		t.Fatalf("minVersion=3 returned %v, want %v — the filter was ignored", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("minVersion=3 returned %v, want %v", got, want)
		}
	}

	// Past the end of the stream is empty, not everything.
	if got := list(numVersions); len(got) != 0 {
		t.Fatalf("minVersion=%d returned %v, want nothing", numVersions, got)
	}
}
