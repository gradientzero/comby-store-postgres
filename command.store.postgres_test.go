package store_test

import (
	"context"
	"testing"

	store "github.com/gradientzero/comby-store-postgres"
	"github.com/gradientzero/comby/v2"
)

func TestCommandStore1(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	commandStore := store.NewCommandStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")
	if err = commandStore.Init(ctx,
		comby.CommandStoreOptionWithAttribute("key1", "value"),
	); err != nil {
		t.Fatal(err)
	}

	// check if the attribute is set
	if v := commandStore.Options().Attributes.Get("key1"); v != nil {
		if v != "value" {
			t.Fatalf("wrong value: %q", v)
		}
	} else {
		t.Fatalf("missing key")
	}

	// reset database
	if err := commandStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// check info with empty store
	if m, err := commandStore.Info(ctx); err != nil {
		t.Fatalf("failed to get info: %v", err)
	} else {
		if m.LastItemCreatedAt != 0 {
			t.Fatalf("wrong last item created at %d", m.LastItemCreatedAt)
		}
	}

	// check totals
	if commandStore.Total(ctx) != 0 {
		t.Fatalf("wrong total %d", commandStore.Total(ctx))
	}

	// Create values
	cmd1 := &comby.BaseCommand{
		CommandUuid: comby.NewUuid(),
		TenantUuid:  "TenantUuid_1",
		Domain:      "Domain_1",
		CreatedAt:   1000,
	}
	if err := commandStore.Create(ctx,
		comby.CommandStoreCreateOptionWithCommand(cmd1),
	); err != nil {
		t.Fatal(err)
	}
	cmd2 := &comby.BaseCommand{
		CommandUuid: comby.NewUuid(),
		TenantUuid:  "TenantUuid_2",
		Domain:      "Domain_2",
		CreatedAt:   1000,
	}
	if err := commandStore.Create(ctx,
		comby.CommandStoreCreateOptionWithCommand(cmd2),
		comby.CommandStoreCreateOptionWithAttribute("anyKey1", "anyValue1"),
	); err != nil {
		t.Fatal(err)
	}

	// check totals
	if commandStore.Total(ctx) != 2 {
		t.Fatalf("wrong total %d", commandStore.Total(ctx))
	}

	// Get a value
	if _cmd1, err := commandStore.Get(ctx,
		comby.CommandStoreGetOptionWithCommandUuid(cmd1.CommandUuid),
	); err != nil {
		t.Fatal(err)
	} else {
		if _cmd1.GetTenantUuid() != "TenantUuid_1" {
			t.Fatalf("wrong value: %q", _cmd1)
		}
	}

	// List all commands
	if evts, total, err := commandStore.List(ctx); err != nil {
		if len(evts) != 2 {
			t.Fatalf("wrong number of commands: %d", len(evts))
		}
		if int64(len(evts)) != total {
			t.Fatalf("wrong number of totals: %d", total)
		}
	}

	// Delete an event
	if err := commandStore.Delete(ctx,
		comby.CommandStoreDeleteOptionWithCommandUuid(cmd1.CommandUuid),
	); err != nil {
		t.Fatal(err)
	}

	// check totals
	if commandStore.Total(ctx) != 1 {
		t.Fatalf("wrong total %d", commandStore.Total(ctx))
	}

	// reset database
	if err := commandStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := commandStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

func TestCommandStoreEncrypted(t *testing.T) {
	var err error
	ctx := context.Background()

	// create crypto service
	key := []byte("12345678901234567890123456789012")
	cryptoService, _ := comby.NewCryptoService(key)

	// setup and init store
	commandStore := store.NewCommandStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")
	if err = commandStore.Init(ctx,
		comby.CommandStoreOptionWithCryptoService(cryptoService),
	); err != nil {
		t.Fatal(err)
	}

	// create domain data to encrypt/decrypt
	type MyDomainCommand struct {
		String  string
		Int     int64
		Boolean bool
	}
	domainData := &MyDomainCommand{
		String:  "string",
		Int:     123,
		Boolean: true,
	}

	// Create values
	cmd1 := &comby.BaseCommand{
		CommandUuid: comby.NewUuid(),
		TenantUuid:  "TenantUuid_1",
		Domain:      "Domain_1",
		CreatedAt:   1000,
		DomainCmd:   domainData,
	}
	if err := commandStore.Create(ctx,
		comby.CommandStoreCreateOptionWithCommand(cmd1),
	); err != nil {
		t.Fatal(err)
	}

	// Get a value
	if _cmd1, err := commandStore.Get(ctx,
		comby.CommandStoreGetOptionWithCommandUuid(cmd1.CommandUuid),
	); err != nil {
		t.Fatal(err)
	} else {
		_domainData := &MyDomainCommand{}
		_domainData, _ = comby.Deserialize(_cmd1.GetDomainCmdBytes(), _domainData)
		if _domainData.String != "string" {
			t.Fatalf("wrong value: %q", _domainData.String)
		}
		if _domainData.Int != 123 {
			t.Fatalf("wrong value: %q", _domainData.Int)
		}
		if _domainData.Boolean != true {
			t.Fatalf("wrong value")
		}
	}

	// List all commands
	if cmds, _, err := commandStore.List(ctx); err == nil {
		_cmd1 := cmds[0]
		_domainData := &MyDomainCommand{}
		_domainData, _ = comby.Deserialize(_cmd1.GetDomainCmdBytes(), _domainData)
		if _domainData.String != "string" {
			t.Fatalf("wrong value: %q", _domainData.String)
		}
		if _domainData.Int != 123 {
			t.Fatalf("wrong value: %q", _domainData.Int)
		}
		if _domainData.Boolean != true {
			t.Fatalf("wrong value")
		}
	}

	// Update event
	domainData.String = "string2"
	domainData.Int = 456
	domainData.Boolean = false
	cmd1.DomainCmd = domainData

	// Delete an event
	if err := commandStore.Update(ctx,
		comby.CommandStoreUpdateOptionWithCommand(cmd1),
	); err != nil {
		t.Fatal(err)
	}

	// Get a value
	if _cmd1, err := commandStore.Get(ctx,
		comby.CommandStoreGetOptionWithCommandUuid(cmd1.CommandUuid),
	); err != nil {
		t.Fatal(err)
	} else {
		_domainData := &MyDomainCommand{}
		_domainData, _ = comby.Deserialize(_cmd1.GetDomainCmdBytes(), _domainData)
		if _domainData.String != "string2" {
			t.Fatalf("wrong value: %q", _domainData.String)
		}
		if _domainData.Int != 456 {
			t.Fatalf("wrong value: %q", _domainData.Int)
		}
		if _domainData.Boolean != false {
			t.Fatalf("wrong value")
		}
	}

	// reset database
	if err := commandStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := commandStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}
