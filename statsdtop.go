package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/wsxiaoys/terminal"
	// "github.com/wsxiaoys/terminal/color"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	VERSION                 = "0.0.1"
	MAX_UNPROCESSED_PACKETS = 1000
	MAX_UDP_PACKET_SIZE     = 512
)

var signalchan chan os.Signal

type Packet struct {
	Bucket   string
	Value    interface{}
	Modifier string
	Sampling float32
}

type Uint64Slice []uint64

func (s Uint64Slice) Len() int           { return len(s) }
func (s Uint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Uint64Slice) Less(i, j int) bool { return s[i] < s[j] }

type Percentiles []*Percentile
type Percentile struct {
	float float64
	str   string
}

func (a *Percentiles) Set(s string) error {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*a = append(*a, &Percentile{f, strings.Replace(s, ".", "_", -1)})
	return nil
}
func (p *Percentile) String() string {
	return p.str
}
func (a *Percentiles) String() string {
	return fmt.Sprintf("%v", *a)
}

var (
	serviceAddress   = flag.String("address", ":8125", "UDP service address")
	flushInterval    = flag.Int64("flush-interval", 10, "Flush interval (seconds)")
	debug            = flag.Bool("debug", false, "print statistics sent to graphite")
	showVersion      = flag.Bool("version", false, "print version string")
	persistCountKeys = flag.Int64("persist-count-keys", 60, "number of flush-interval's to persist count keys")
	percentThreshold = Percentiles{}
)

func init() {
	flag.Var(&percentThreshold, "percent-threshold", "Threshold percent (0-100, may be given multiple times)")
}

var (
	In       = make(chan *Packet, MAX_UNPROCESSED_PACKETS)
	counters = make(map[string]int64)
	gauges   = make(map[string]uint64)
	timers   = make(map[string]Uint64Slice)
)

func monitor() {
	for {
		select {
		case sig := <-signalchan:
			fmt.Printf("!! Caught signal %d... shutting down\n", sig)
			return
		case s := <-In:
			if s.Modifier == "ms" {
				_, ok := timers[s.Bucket]
				if !ok {
					var t Uint64Slice
					timers[s.Bucket] = t
				}
				timers[s.Bucket] = append(timers[s.Bucket], s.Value.(uint64))
			} else if s.Modifier == "g" {
				gauges[s.Bucket] = s.Value.(uint64)
			} else {
				v, ok := counters[s.Bucket]
				if !ok || v < 0 {
					counters[s.Bucket] = 0
				}
				counters[s.Bucket] += int64(float64(s.Value.(int64)) * float64(1/s.Sampling))
			}
		}
	}
}

var packetRegexp = regexp.MustCompile("^([^:]+):(-?[0-9]+)\\|(g|c|ms)(\\|@([0-9\\.]+))?\n?$")

func parseMessage(data []byte) []*Packet {
	var output []*Packet
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(line) == 0 {
			continue
		}

		item := packetRegexp.FindSubmatch(line)
		if len(item) == 0 {
			continue
		}

		var err error
		var value interface{}
		modifier := string(item[3])
		switch modifier {
		case "c":
			value, err = strconv.ParseInt(string(item[2]), 10, 64)
			if err != nil {
				log.Printf("ERROR: failed to ParseInt %s - %s", item[2], err)
				continue
			}
		default:
			value, err = strconv.ParseUint(string(item[2]), 10, 64)
			if err != nil {
				log.Printf("ERROR: failed to ParseUint %s - %s", item[2], err)
				continue
			}
		}

		sampleRate, err := strconv.ParseFloat(string(item[5]), 32)
		if err != nil {
			sampleRate = 1
		}

		packet := &Packet{
			Bucket:   string(item[1]),
			Value:    value,
			Modifier: modifier,
			Sampling: float32(sampleRate),
		}
		output = append(output, packet)
	}
	return output
}

func udpListener() {
	address, _ := net.ResolveUDPAddr("udp", *serviceAddress)
	log.Printf("listening on %s", address)
	listener, err := net.ListenUDP("udp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenUDP - %s", err)
	}
	defer listener.Close()

	message := make([]byte, MAX_UDP_PACKET_SIZE)
	for {
		n, remaddr, err := listener.ReadFromUDP(message)
		if err != nil {
			log.Printf("ERROR: reading UDP packet from %+v - %s", remaddr, err)
			continue
		}

		for _, p := range parseMessage(message[:n]) {
			In <- p
		}
	}
}

func top() {
	for {
		terminal.Stdout.
			Clear().
			Move(1,0).
			Color("y").
			Print(fmt.Sprintf("%v", time.Now().Unix())).
			Move(2,0).
			Color("r").
			Print("Counters").
			Right(4).
			Color("b")
		
		var x = 4
		var y = 3
		
		for s, c := range counters {
			terminal.Stdout.
				Move(y, x).
				Print(fmt.Sprintf("%s: %d", s, c))
				
			y = y + 1
		}
		
	
		time.Sleep(time.Second)
	}
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("statsdaemon v%s (built w/%s)\n", VERSION, runtime.Version())
		return
	}

	signalchan = make(chan os.Signal, 1)
	signal.Notify(signalchan, syscall.SIGTERM)
	*persistCountKeys = -1 * (*persistCountKeys)

	go udpListener()
	go top()
	monitor()
}
