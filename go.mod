module github.com/gradientzero/comby-store-postgres

go 1.22

replace github.com/gradientzero/comby/v2 v2.0.0 => /Users/me/Documents/gradient0/repos/comby/comby

require (
	github.com/gradientzero/comby/v2 v2.0.0
	github.com/lib/pq v1.10.9
)

require github.com/huandu/go-clone v1.7.2 // indirect
