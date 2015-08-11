package turnpike

import "sync"

// A broker handles routing EVENTS from Publishers to Subscribers.
type Broker interface {
	// Publishes a message to all Subscribers.
	Publish(Sender, *Publish)
	// Subscribes to messages on a URI.
	Subscribe(Sender, *Subscribe)
	// Unsubscribes from messages on a URI.
	Unsubscribe(Sender, *Unsubscribe)
}

// A super simple broker that matches URIs to Subscribers.
type defaultBroker struct {
	lock          sync.RWMutex
	options       map[URI]map[ID]map[string]interface{}
	routes        map[URI]map[ID]Sender
	subscriptions map[ID]URI
}

// NewDefaultBroker initializes and returns a simple broker that matches URIs to Subscribers.
func NewDefaultBroker() Broker {
	return &defaultBroker{
		options:       make(map[URI]map[ID]map[string]interface{}),
		routes:        make(map[URI]map[ID]Sender),
		subscriptions: make(map[ID]URI),
	}
}

// Publish sends a message to all subscribed clients except for the sender.
//
// If msg.Options["acknowledge"] == true, the publisher receives a Published event
// after the message has been sent to all subscribers.
func (br *defaultBroker) Publish(pub Sender, msg *Publish) {
	pubId := NewID()
	evtTemplate := Event{
		Publication: pubId,
		Arguments:   msg.Arguments,
		ArgumentsKw: msg.ArgumentsKw,
		Details:     make(map[string]interface{}),
	}

	br.lock.RLock()
	defer br.lock.RUnlock()
subscriber:
	for id, sub := range br.routes[msg.Topic] {
		// don't send event to publisher
		if sub == pub {
			continue
		}

		subOptions := br.options[msg.Topic][id]
		for option, pubValue := range msg.Options {
			if subValue, ok := subOptions[option]; ok && subValue != pubValue {
				continue subscriber
			}
		}

		// shallow-copy the template
		event := evtTemplate
		event.Subscription = id
		sub.Send(&event)
	}

	// only send published message if acknowledge is present and set to true
	if doPub, _ := msg.Options["acknowledge"].(bool); doPub {
		pub.Send(&Published{Request: msg.Request, Publication: pubId})
	}
}

// Subscribe subscribes the client to the given topic.
func (br *defaultBroker) Subscribe(sub Sender, msg *Subscribe) {
	br.lock.Lock()
	defer br.lock.Unlock()

	id := NewID()
	route, ok := br.routes[msg.Topic]
	if !ok {
		br.routes[msg.Topic] = make(map[ID]Sender)
		route = br.routes[msg.Topic]
	}
	route[id] = sub

	option, ok := br.options[msg.Topic]
	if !ok {
		br.options[msg.Topic] = make(map[ID]map[string]interface{})
		option = br.options[msg.Topic]
	}
	option[id] = msg.Options

	br.subscriptions[id] = msg.Topic

	sub.Send(&Subscribed{Request: msg.Request, Subscription: id})
}

func (br *defaultBroker) Unsubscribe(sub Sender, msg *Unsubscribe) {
	br.lock.Lock()
	defer br.lock.Unlock()

	topic, ok := br.subscriptions[msg.Subscription]
	if !ok {
		err := &Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Error:   ErrNoSuchSubscription,
		}
		sub.Send(err)
		log.Printf("Error unsubscribing: no such subscription %v", msg.Subscription)
		return
	}
	delete(br.subscriptions, msg.Subscription)

	// clean up routes
	if r, ok := br.routes[topic]; !ok {
		log.Printf("Error unsubscribing: unable to find routes for %s topic", topic)
	} else if _, ok := r[msg.Subscription]; !ok {
		log.Printf("Error unsubscribing: %s route does not exist for %v subscription", topic, msg.Subscription)
	} else {
		delete(r, msg.Subscription)
		if len(r) == 0 {
			delete(br.routes, topic)
		}
	}

	// clean up options
	if o, ok := br.options[topic]; !ok {
		log.Printf("Error unsubscribing: unable to find options for %s topic", topic)
	} else if _, ok := o[msg.Subscription]; !ok {
		log.Printf("Error unsubscribing: %s options does not exist for %v subscription", topic, msg.Subscription)
	} else {
		delete(o, msg.Subscription)
		if len(o) == 0 {
			delete(br.options, topic)
		}
	}

	sub.Send(&Unsubscribed{Request: msg.Request})
}
