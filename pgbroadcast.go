package pgbroadcast

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

func Run(pgconninfo string, pglistenchannel string) error {
	go h.run()
	err := startPgListener(pgconninfo, pglistenchannel, h.broadcast)
	return err
}

func startPgListener(pgconninfo string, pglistenchannel string, broadcastingchannel chan pgnotification) error {
	// callback func
	pgEventCallback := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Println("pgbroadcast: ", err.Error())
		}
	}

	// create listener and start listening
	l := pq.NewListener(pgconninfo, 10*time.Second, time.Minute, pgEventCallback)

	err := l.Listen(pglistenchannel)
	if err != nil {
		return err
	}
	// Wait for notifications in goroutine, pass them to the broadcastingchannel.
	go waitForNotification(l)
	fmt.Println("pgbroadcast: listening for notifications")
	return nil
}

func waitForNotification(l *pq.Listener) {
	for {
		select {
		case n := <-l.Notify:
			//
			if n == nil {
				continue
			}
			// Unmarshal JSON in pgnotification struct
			var pgn pgnotification
			err := json.Unmarshal([]byte(n.Extra), &pgn)
			if err != nil {
				fmt.Println("pgbroadcast: error processing JSON: ", err)
			} else {
				h.broadcast <- pgn
			}
		case <-time.After(60 * time.Second):
			// received no events for 60 seconds, ping connection")
			go func() {
				l.Ping()
			}()
		}
	}
}
