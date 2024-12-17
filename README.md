# comby-postgres-store

Implementation of the EventStore and CommandStore interfaces defined in [comby](https://github.com/gradientzero/comby) with Postgres. **comby** is a powerful application framework designed with Event Sourcing and Command Query Responsibility Segregation (CQRS) principles, written in Go.

[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

## Prerequisites

- [Golang 1.22+](https://go.dev/dl/)
- [comby](https://github.com/gradientzero/comby)
- [Postgres](https://www.postgresql.org/download/)

```shell
# run postgres locally for testings
docker run --name some-postgres -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres
```

## Installation

*comby-postgres-store* supports the latest version of comby (v2), requires Go version 1.22+ and is based on Postgres driver [lib/pq](https://github.com/lib/pq).

```shell
go get github.com/gradientzero/comby-postgres-store
```

## Quickstart

```go
import (
	"github.com/gradientzero/comby-postgres-store"
	"github.com/gradientzero/comby/v2"
)

// create postgres CommandStore
commandStore := store.NewCommandStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")
if err = commandStore.Init(ctx,
    comby.CommandStoreOptionWithAttribute("anyKey", "anyValue"),
); err != nil {
    panic(err)
}
// create postgres EventStore
eventStore := store.NewEventStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")
if err = eventStore.Init(ctx,
    comby.EventStoreOptionWithAttribute("anyKey", "anyValue"),
); err != nil {
    panic(err)
}

// create Facade
fc, _ := comby.NewFacade(
  comby.FacadeWithCommandStore(commandStore),
  comby.FacadeWithEventStore(eventStore),
)
```

## Tests

```shell
go test -v ./...
```

## Contributing
Please follow the guidelines in [CONTRIBUTING.md](./CONTRIBUTING.md).

## License
This project is licensed under the [MIT License](./LICENSE.md).

## Contact
https://www.gradient0.com
