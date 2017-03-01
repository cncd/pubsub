package pubsub

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Filter is used to filter messages. It returns true if the message
// should be distributed to the consumer.
type Filter func(Message) bool

type handler struct {
	publisher Publisher
	filter    Filter
	topic     string
}

// Handler returns an http.Handler
func Handler(topic string, filter Filter) http.Handler {
	return &handler{
		publisher: global,
		filter:    filter,
		topic:     topic,
	}
}

// ServeHTTP upgrades the incomiong http.Request to a websocket and writes
// all published messages to the websocket connection.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		if _, ok := err.(websocket.HandshakeError); !ok {
			log.Println(err)
		}
		return
	}
	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	tick := time.NewTicker(pingPeriod)
	queue := make(chan Message, 10)

	defer func() {
		cancel()
		tick.Stop()
		ws.Close()
	}()
	go func() {
		h.publisher.Subscribe(ctx, h.topic, func(message Message) {
			queue <- message
		})
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-queue:
				ws.SetWriteDeadline(time.Now().Add(writeWait))
				ws.WriteMessage(websocket.TextMessage, message.Data)
			case <-tick.C:
				err := ws.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(writeWait))
				if err != nil {
					return
				}
			}
		}
	}()
	reader(ws)
}

var (
	// Time allowed to write the file to the client.
	writeWait = 5 * time.Second

	// Time allowed to read the next pong message from the client.
	pongWait = 60 * time.Second

	// Send pings to client with this period. Must be less than pongWait.
	pingPeriod = 30 * time.Second

	// upgrader defines the default behavior for upgrading the websocket.
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

func reader(ws *websocket.Conn) {
	defer ws.Close()
	ws.SetReadLimit(512)
	ws.SetReadDeadline(time.Now().Add(pongWait))
	ws.SetPongHandler(func(string) error {
		ws.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		_, _, err := ws.ReadMessage()
		if err != nil {
			break
		}
	}
}
