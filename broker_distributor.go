package turnpike

// BrokerDistributor is the interface that intercepts broker events
type BrokerDistributor interface {
	ShouldPublish(pub, sub *Session, options map[string]interface{}, msg *Publish) bool
}

type defaultBrokerDistributor struct{}

// NewDefaultBrokerInterceptor returns a simple broker interceptor
func NewDefaultBrokerDistributor() *defaultBrokerDistributor {
	return &defaultBrokerDistributor{}
}

func (bi *defaultBrokerDistributor) ShouldPublish(pub, sub *Session, options map[string]interface{}, msg *Publish) bool {
	// don't send event to publisher
	if sub == pub {
		return false
	}

	for option, pubValue := range msg.Options {
		if subValue, ok := options[option]; ok && subValue != pubValue {
			return false
		}
	}

	return true
}
