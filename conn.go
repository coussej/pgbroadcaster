package pgbroadcast

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512
)

// the websocket upgrader.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// connection is a middleman between the websocket connection and the hub.
// it contains the actual websocket connection, a map where we keep the tables
// this connection is monitoring, and a send channel in which the  hub will
// write the outbound notifications.
type connection struct {
	ws            *websocket.Conn
	subscriptions map[string]bool
	send          chan pgnotification
}

// the reader listens for incoming messages on the websocket. the purpose is
// that the client sends a string message containing the tablename it wants to
// monitor.
func (c *connection) reader() {
	defer func() {
		h.unregister <- c
		c.ws.Close()
	}()
	c.ws.SetReadLimit(maxMessageSize)
	c.ws.SetReadDeadline(time.Now().Add(pongWait))
	c.ws.SetPongHandler(func(string) error { c.ws.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	for {
		t, m, err := c.ws.ReadMessage()
		if err != nil || t != websocket.TextMessage {
			break
		}
		c.subscriptions[string(m)] = true
		fmt.Println("pgbroadcast: incoming subscription for table [", string(m), "]")
	}
}

// writeMessage writes a message with the given message type and payload.
func (c *connection) writeMessage(mt int, payload []byte) error {
	c.ws.SetWriteDeadline(time.Now().Add(writeWait))
	return c.ws.WriteMessage(mt, payload)
}

// writeMessage marshals an interface to json and sends it.
func (c *connection) writeJSON(i interface{}) error {
	c.ws.SetWriteDeadline(time.Now().Add(writeWait))
	return c.ws.WriteJSON(i)
}

// writer writes the pgnotificiations it receives from the hub to the websocket
// connection.
func (c *connection) writer() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.ws.Close()
	}()
	for {
		select {
		case notification, ok := <-c.send:
			if !ok {
				c.writeMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.writeJSON(notification); err != nil {
				return
			}
		case <-ticker.C:
			if err := c.writeMessage(websocket.PingMessage, []byte{}); err != nil {
				return
			}
		}
	}
}

// serverWs handles websocket requests from the peer.
func ServeWs(w http.ResponseWriter, r *http.Request) {
	// Accept only GET requests
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	fmt.Println("pgbroadcast: incoming client connection", r.Header["User-Agent"])

	// upgrade the connection to a websocket connection
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	// create a new connection struct and add it to the hub.
	c := &connection{
		ws:            ws,
		subscriptions: make(map[string]bool),
		send:          make(chan pgnotification, 1024),
	}
	h.register <- c

	// start the connections reader and writer.
	go c.writer()
	c.reader()
}
