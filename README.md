# pgbroadcast
Example app that shows how to send postgres JSON notifictions over websockets

## Usage

``` go
package main

import (
	"log"
	"net/http"

	"github.com/coussej/pgbroadcast"
)

func main() {
  // call the Run method, using the connection info as first argument and the 
  // channel name as second.
	err := pgbroadcast.Run("dbname=exampledb user=webapp password=webapp", "events")
	if err != nil {
		log.Fatal(err)
	}

	// use the exposed webhandler in your webserver.
	http.HandleFunc("/ws", pgbroadcast.ServeWs)

	err = http.ListenAndServe(":6060", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

```