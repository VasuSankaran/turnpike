package turnpike

// BrokerDistributor is the interface that intercepts broker events
type BrokerDistributor interface {
	GetPublishRoutes(pub *Session, routes map[ID]*Route, msg *Publish) map[ID]*Route
}

type defaultBrokerDistributor struct{}

// NewDefaultBrokerInterceptor returns a simple broker interceptor
func NewDefaultBrokerDistributor() *defaultBrokerDistributor {
	return &defaultBrokerDistributor{}
}

func (bi *defaultBrokerDistributor) GetPublishRoutes(pub *Session, routes map[ID]*Route, msg *Publish) map[ID]*Route {
	sendRoutes := make(map[ID]*Route)

outer:
	for id, route := range routes {
		// don't send event to publisher
		if route.Session == pub {
			continue
		}

		for option, pubValue := range msg.Options {
			if subValue, ok := route.Options[option]; ok && subValue != pubValue {
				continue outer
			}
		}

		sendRoutes[id] = route
	}

	return sendRoutes
}
