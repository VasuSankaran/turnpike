package turnpike

import (
	"net"
)
type NetDialFunc func(network, addr string) (net.Conn, error)
