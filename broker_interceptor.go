package turnpike

// BrokerInterceptor is the interface that intercepts broker events
type BrokerInterceptor interface {
	ShouldPublish(pub, sub *Session, options map[string]interface{}, msg *Publish) bool
}

type defaultBrokerInterceptor struct{}

// NewDefaultBrokerInterceptor returns a simple broker interceptor
func NewDefaultBrokerInterceptor() *defaultBrokerInterceptor {
	return &defaultBrokerInterceptor{}
}

func (bi *defaultBrokerInterceptor) ShouldPublish(pub, sub *Session, options map[string]interface{}, msg *Publish) bool {
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
