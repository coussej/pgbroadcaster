// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pgbroadcast

import (
	"fmt"
	"log"
	"net/http"
	"strings"
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

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// connection is a middleman between the websocket connection and the hub.
type connection struct {
	// The websocket connection.
	ws *websocket.Conn

	// The tables from which this connections wants updates
	pgtables []string

	// Buffered channel of outbound notifications.
	send chan pgnotification
}

// has subscription checks returns true is the connection registered to a
// specific pgtable.
func (c *connection) hasSubscription(pgtable string) bool {
	for _, v := range c.pgtables {
		if v == pgtable {
			return true
		}
	}
	return false
}

// reader is here just for show. We will print the messages to the console,
// but we don't want any incoming trafic. Shut up and listen, websocket!
func (c *connection) reader() {
	defer func() {
		h.unregister <- c
		c.ws.Close()
	}()
	c.ws.SetReadLimit(maxMessageSize)
	c.ws.SetReadDeadline(time.Now().Add(pongWait))
	c.ws.SetPongHandler(func(string) error { c.ws.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	for {
		_, message, err := c.ws.ReadMessage()
		if err != nil {
			break
		}
		fmt.Println("pgbroadcast: incoming message from ws: ", string(message))
	}
}

// writeMessage writes a message with the given message type and payload.
func (c *connection) writeMessage(mt int, payload []byte) error {
	c.ws.SetWriteDeadline(time.Now().Add(writeWait))
	return c.ws.WriteMessage(mt, payload)
}

func (c *connection) writeJSON(i interface{}) error {
	c.ws.SetWriteDeadline(time.Now().Add(writeWait))
	return c.ws.WriteJSON(i)
}

// writer writes messages from the hub to the websocket connection.
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

	// Check if the querystring contains a parameter 'tables'
	t := r.URL.Query().Get("tables")
	if t == "" {
		http.Error(w, "Querystring does not contain parameter 'tables'.", 400)
		return
	}
	fmt.Println("pqbroadcast: client connection received: ", t)
	// Split the csv parameter to a slice.
	tables := strings.Split(t, ",")
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	c := &connection{ws: ws, pgtables: tables, send: make(chan pgnotification, 1024)}
	h.register <- c
	go c.writer()
	c.reader()
}
