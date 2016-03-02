package tcp_listener

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers"
)

type TcpListener struct {
	ServiceAddress         string
	AllowedPendingMessages int
	MaxTCPConnections      int `toml:"max_tcp_connections"`
	TCPPacketSize          int `toml:"tcp_packet_size"`

	sync.Mutex

	in     chan []byte
	done   chan struct{}
	accept chan bool

	parser parsers.Parser

	// Keep the accumulator in this struct
	acc telegraf.Accumulator
}

var dropwarn = "ERROR: Message queue full. Discarding line [%s] " +
	"You may want to increase allowed_pending_messages in the config\n"

const sampleConfig = `
  ## Address and port to host TCP listener on
  service_address = ":8094"

  ## Number of TCP messages allowed to queue up. Once filled, the
  ## TCP listener will start dropping packets.
  allowed_pending_messages = 10000

  ## TCP packet size for the server to listen for. This will depend
  ## on the size of the packets that the client is sending.
  tcp_packet_size = 1500

  ## Maximum number of concurrent TCP connections to allow
  max_tcp_connections = 250

  ## Data format to consume. This can be "json", "influx" or "graphite"
  ## Each data format has it's own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
`

func (t *TcpListener) SampleConfig() string {
	return sampleConfig
}

func (t *TcpListener) Description() string {
	return "Generic TCP listener"
}

// All the work is done in the Start() function, so this is just a dummy
// function.
func (t *TcpListener) Gather(_ telegraf.Accumulator) error {
	return nil
}

func (t *TcpListener) SetParser(parser parsers.Parser) {
	t.parser = parser
}

func (t *TcpListener) Start(acc telegraf.Accumulator) error {
	t.Lock()
	defer t.Unlock()

	t.acc = acc
	t.in = make(chan []byte, t.AllowedPendingMessages)
	t.done = make(chan struct{})
	t.accept = make(chan bool, t.MaxTCPConnections)
	for i := 0; i < t.MaxTCPConnections; i++ {
		t.accept <- true
	}

	go t.tcpListen()
	go t.tcpParser()

	log.Printf("Started TCP listener service on %s\n", t.ServiceAddress)
	return nil
}

func (t *TcpListener) Stop() {
	t.Lock()
	defer t.Unlock()
	close(t.done)
	close(t.in)
	log.Println("Stopped TCP listener service on ", t.ServiceAddress)
}

func (t *TcpListener) tcpListen() error {
	// Start listener
	address, _ := net.ResolveTCPAddr("tcp", t.ServiceAddress)
	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenUDP - %s", err)
	}
	defer listener.Close()
	log.Println("TCP server listening on: ", listener.Addr().String())

	for {
		select {
		case <-t.done:
			return nil
		default:
			// Accept connection:
			conn, err := listener.AcceptTCP()
			if err != nil {
				return err
			}

			log.Printf("Received TCP Connection from %s", conn.RemoteAddr())

			select {
			case <-t.accept:
				// not over connection limit, handle the connection properly.
				go t.handler(conn)
			default:
				// We are over the connection limit, refuse & close.
				t.refuser(conn)
			}
		}
	}
}

func (t *TcpListener) refuser(conn *net.TCPConn) {
	// Tell the connection why we are closing.
	fmt.Fprintf(conn, "Telegraf maximum concurrent TCP connections (%d)"+
		" reached, closing.\nYou may want to increase max_tcp_connections in"+
		" the Telegraf tcp listener configuration.\n", t.MaxTCPConnections)
	conn.Close()
	log.Printf("Closed TCP Connection from %s", conn.RemoteAddr())
	log.Printf("WARNING: Maximum TCP Connections reached, you may want to" +
		" adjust max_tcp_connections")
}

func (t *TcpListener) handler(conn *net.TCPConn) {
	defer conn.Close()
	defer log.Printf("Closed TCP Connection from %s", conn.RemoteAddr())
	// Add one connection potential back to channel when this one closes
	defer func() { t.accept <- true }()

	for {
		select {
		case <-t.done:
			return
		default:
			buf := make([]byte, t.TCPPacketSize)
			n, err := conn.Read(buf)
			if err != nil {
				if err == io.EOF {
					return
				}
				log.Printf("Error reading message: %s", err)
			}
			select {
			case t.in <- bytes.TrimSpace(buf[:n]):
			default:
				log.Printf(dropwarn, string(buf[:n]))
			}
		}
	}
}

func (t *TcpListener) tcpParser() error {
	for {
		select {
		case <-t.done:
			return nil
		case packet := <-t.in:
			if len(packet) == 0 {
				continue
			}
			metrics, err := t.parser.Parse(packet)
			if err == nil {
				t.storeMetrics(metrics)
			} else {
				log.Printf("Malformed packet: [%s], Error: %s\n",
					string(packet), err)
			}
		}
	}
}

func (t *TcpListener) storeMetrics(metrics []telegraf.Metric) error {
	t.Lock()
	defer t.Unlock()
	for _, m := range metrics {
		t.acc.AddFields(m.Name(), m.Fields(), m.Tags(), m.Time())
	}
	return nil
}

func init() {
	inputs.Add("tcp_listener", func() telegraf.Input {
		return &TcpListener{}
	})
}
