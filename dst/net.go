package dst

import (
	"context"
	"net"
)

// Net is a network between machines, with the net package's interfaces.
// The real World's is the operating system's; a simulated World's
// connects simulated machines by name, with latency, partitions and
// connection failures.
type Net interface {
	Listen(network, address string) (net.Listener, error)
	Dial(network, address string) (net.Conn, error)
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}
