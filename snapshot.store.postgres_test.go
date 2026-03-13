package store_test

import (
	"context"
	"sync"
	"testing"

	store "github.com/gradientzero/comby-store-postgres"
	"github.com/gradientzero/comby/v2"
)

func newSnapshotStorePostgres(t *testing.T) comby.SnapshotStore {
	t.Helper()
	ctx := context.Background()
	s := store.NewSnapshotStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")
	if err := s.Init(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSnapshotStorePostgres_SaveAndGetLatest(t *testing.T) {
	ctx := context.Background()
	s := newSnapshotStorePostgres(t)
	defer s.Close(ctx)

	uuid := comby.NewUuid()
	model := &comby.SnapshotStoreModel{
		AggregateUuid: uuid,
		Domain:        "TestDomain",
		Version:       42,
		Data:          []byte(`{"value":"hello"}`),
		CreatedAt:     1000,
	}

	if err := s.Save(ctx, model); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetLatest(ctx, uuid)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected snapshot, got nil")
	}
	if got.AggregateUuid != uuid {
		t.Errorf("expected aggregateUuid %s, got %s", uuid, got.AggregateUuid)
	}
	if got.Domain != "TestDomain" {
		t.Errorf("expected domain TestDomain, got %s", got.Domain)
	}
	if got.Version != 42 {
		t.Errorf("expected version 42, got %d", got.Version)
	}
	if string(got.Data) != `{"value":"hello"}` {
		t.Errorf("expected data, got %s", string(got.Data))
	}
	if got.CreatedAt != 1000 {
		t.Errorf("expected createdAt 1000, got %d", got.CreatedAt)
	}

	// cleanup
	s.Delete(ctx, uuid)
}

func TestSnapshotStorePostgres_GetLatest_NotFound(t *testing.T) {
	ctx := context.Background()
	s := newSnapshotStorePostgres(t)
	defer s.Close(ctx)

	got, err := s.GetLatest(ctx, comby.NewUuid())
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("expected nil for non-existent snapshot, got %+v", got)
	}
}

func TestSnapshotStorePostgres_SaveNil(t *testing.T) {
	ctx := context.Background()
	s := newSnapshotStorePostgres(t)
	defer s.Close(ctx)

	if err := s.Save(ctx, nil); err == nil {
		t.Error("expected error when saving nil snapshot")
	}
}

func TestSnapshotStorePostgres_Upsert(t *testing.T) {
	ctx := context.Background()
	s := newSnapshotStorePostgres(t)
	defer s.Close(ctx)

	uuid := comby.NewUuid()

	s.Save(ctx, &comby.SnapshotStoreModel{
		AggregateUuid: uuid,
		Domain:        "Test",
		Version:       10,
		Data:          []byte(`v10`),
		CreatedAt:     1000,
	})

	s.Save(ctx, &comby.SnapshotStoreModel{
		AggregateUuid: uuid,
		Domain:        "Test",
		Version:       20,
		Data:          []byte(`v20`),
		CreatedAt:     2000,
	})

	got, _ := s.GetLatest(ctx, uuid)
	if got.Version != 20 {
		t.Errorf("expected version 20 after upsert, got %d", got.Version)
	}
	if string(got.Data) != "v20" {
		t.Errorf("expected data v20, got %s", string(got.Data))
	}

	// cleanup
	s.Delete(ctx, uuid)
}

func TestSnapshotStorePostgres_Delete(t *testing.T) {
	ctx := context.Background()
	s := newSnapshotStorePostgres(t)
	defer s.Close(ctx)

	uuid := comby.NewUuid()
	s.Save(ctx, &comby.SnapshotStoreModel{
		AggregateUuid: uuid,
		Domain:        "Test",
		Version:       5,
		Data:          []byte(`data`),
		CreatedAt:     1000,
	})

	if err := s.Delete(ctx, uuid); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetLatest(ctx, uuid)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("expected nil after delete, got %+v", got)
	}
}

func TestSnapshotStorePostgres_DeleteNonExistent(t *testing.T) {
	ctx := context.Background()
	s := newSnapshotStorePostgres(t)
	defer s.Close(ctx)

	if err := s.Delete(ctx, comby.NewUuid()); err != nil {
		t.Errorf("expected no error deleting non-existent snapshot, got %v", err)
	}
}

func TestSnapshotStorePostgres_MultipleAggregates(t *testing.T) {
	ctx := context.Background()
	s := newSnapshotStorePostgres(t)
	defer s.Close(ctx)

	uuid1 := comby.NewUuid()
	uuid2 := comby.NewUuid()

	s.Save(ctx, &comby.SnapshotStoreModel{
		AggregateUuid: uuid1,
		Domain:        "A",
		Version:       1,
		Data:          []byte(`agg1`),
	})
	s.Save(ctx, &comby.SnapshotStoreModel{
		AggregateUuid: uuid2,
		Domain:        "B",
		Version:       2,
		Data:          []byte(`agg2`),
	})

	got1, _ := s.GetLatest(ctx, uuid1)
	got2, _ := s.GetLatest(ctx, uuid2)

	if got1 == nil || string(got1.Data) != "agg1" {
		t.Errorf("expected agg1 data for uuid1")
	}
	if got2 == nil || string(got2.Data) != "agg2" {
		t.Errorf("expected agg2 data for uuid2")
	}

	// delete one, other should remain
	s.Delete(ctx, uuid1)

	got1, _ = s.GetLatest(ctx, uuid1)
	got2, _ = s.GetLatest(ctx, uuid2)
	if got1 != nil {
		t.Errorf("expected nil for deleted uuid1")
	}
	if got2 == nil {
		t.Errorf("expected uuid2 to still exist")
	}

	// cleanup
	s.Delete(ctx, uuid2)
}

func TestSnapshotStorePostgres_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	s := newSnapshotStorePostgres(t)
	defer s.Close(ctx)

	const numGoroutines = 50
	var wg sync.WaitGroup
	uuids := make([]string, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		uuids[i] = comby.NewUuid()
		wg.Add(1)
		go func(idx int, uuid string) {
			defer wg.Done()

			s.Save(ctx, &comby.SnapshotStoreModel{
				AggregateUuid: uuid,
				Domain:        "Test",
				Version:       int64(idx + 1),
				Data:          []byte(`mydata`),
				CreatedAt:     int64(idx),
			})

			got, err := s.GetLatest(ctx, uuid)
			if err != nil {
				t.Errorf("goroutine %d: unexpected error: %v", idx, err)
				return
			}
			if got == nil {
				t.Errorf("goroutine %d: expected snapshot, got nil", idx)
				return
			}
			if got.Version != int64(idx+1) {
				t.Errorf("goroutine %d: expected version %d, got %d", idx, idx+1, got.Version)
			}
		}(i, uuids[i])
	}
	wg.Wait()

	// cleanup
	for _, uuid := range uuids {
		s.Delete(ctx, uuid)
	}
}

func TestSnapshotStorePostgres_LargeData(t *testing.T) {
	ctx := context.Background()
	s := newSnapshotStorePostgres(t)
	defer s.Close(ctx)

	// 1MB blob
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	uuid := comby.NewUuid()
	if err := s.Save(ctx, &comby.SnapshotStoreModel{
		AggregateUuid: uuid,
		Domain:        "Test",
		Version:       1,
		Data:          largeData,
		CreatedAt:     1000,
	}); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetLatest(ctx, uuid)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected snapshot, got nil")
	}
	if len(got.Data) != len(largeData) {
		t.Fatalf("expected %d bytes, got %d", len(largeData), len(got.Data))
	}
	for i := range largeData {
		if got.Data[i] != largeData[i] {
			t.Fatalf("data mismatch at byte %d", i)
		}
	}

	// cleanup
	s.Delete(ctx, uuid)
}
