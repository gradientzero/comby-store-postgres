package store

import (
	"fmt"
	"strings"
)

// buildSSLParams renders the libpq SSL-related DSN fragment shared by the
// event, command and snapshot stores. An empty mode falls back to "disable" to
// preserve the historical default behaviour of the Postgres stores.
// Certificate paths (sslrootcert/sslcert/sslkey) are only emitted when set.
func buildSSLParams(mode, rootCert, cert, key string) string {
	if mode == "" {
		mode = "disable"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "sslmode=%s", mode)
	if rootCert != "" {
		fmt.Fprintf(&b, " sslrootcert=%s", rootCert)
	}
	if cert != "" {
		fmt.Fprintf(&b, " sslcert=%s", cert)
	}
	if key != "" {
		fmt.Fprintf(&b, " sslkey=%s", key)
	}
	return b.String()
}
