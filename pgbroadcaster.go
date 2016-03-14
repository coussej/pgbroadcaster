package pgbroadcaster

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
)

type pgnotification struct {
	Table  string                 `json:"table"`
	Action string                 `json:"action"`
	Data   map[string]interface{} `json:"data"`
}

type PgBroadcaster struct {
	h *hub
	l *pq.Listener
}

func NewPgBroadcaster(pgconninfo string) (*PgBroadcaster, error) {
	// Create a new hub to manage the connections
	var h = hub{
		broadcast:   make(chan pgnotification),
		register:    make(chan *connection),
		unregister:  make(chan *connection),
		connections: make(map[*connection]bool),
	}

	// start the hub in a new go-routine.
	go h.run()

	// Get a new postgres listener connection
	pl, err := newPgListener(pgconninfo)
	if err != nil {
		fmt.Println("pgbroadcaster: ", err)
		return nil, err
	}

	// Create a PgBroadcaster and start handling connections
	pb := &PgBroadcaster{&h, pl}
	go pb.handleIncomingNotifications()

	// Return the pgBroadcaster
	return pb, nil
}

// Listen makes the PgBroadcaster's underlying pglistener listen to thh
// specified channel
func (pb *PgBroadcaster) Listen(pgchannel string) error {
	return pb.l.Listen(pgchannel)
}

// newPgListener creates and returns the pglistener from the pq package.
func newPgListener(pgconninfo string) (*pq.Listener, error) {

	// create a callback function to monitor connection state changes
	pgEventCallback := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Println("pgbroadcast: ", err.Error())
		}
	}

	// create the listener
	l := pq.NewListener(pgconninfo, 10*time.Second, time.Minute, pgEventCallback)

	return l, nil
}

func (pb *PgBroadcaster) handleIncomingNotifications() {
	for {
		select {
		case n := <-pb.l.Notify:
			// For some reason after connection loss with the postgres database,
			// the first notifications is a nil notification. Ignore it.
			if n == nil {
				continue
			}
			// Unmarshal JSON in pgnotification struct
			var pgn pgnotification
			err := json.Unmarshal([]byte(n.Extra), &pgn)
			if err != nil {
				fmt.Println("pgbroadcast: error processing JSON: ", err)
			} else {
				pb.h.broadcast <- pgn
			}
		case <-time.After(60 * time.Second):
			// received no events for 60 seconds, ping connection")
			go func() {
				pb.l.Ping()
			}()
		}
	}
}
