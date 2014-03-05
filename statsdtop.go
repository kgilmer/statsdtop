package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/jroimartin/gocui"
	// "github.com/wsxiaoys/terminal/color"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"sort"
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

type Percentiles []*Percentile
type Percentile struct {
	float float64
	str   string
}

type Counter struct {
	value int64
	received time.Time
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
	counters = make(map[string]Counter)
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
				if !ok || v.value < 0 {
					counters[s.Bucket] = Counter{0, time.Now()}
				}
				val := counters[s.Bucket].value + int64(float64(s.Value.(int64)) * float64(1/s.Sampling))
				counters[s.Bucket] = Counter{val, time.Now()}
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

func layout(g *gocui.Gui) error {
	maxX, maxY := g.Size()
	columnX := maxX/3 

	g.BgColor = gocui.ColorWhite
	g.FgColor = gocui.ColorBlack
	g.RenderFrames = false

	if v, err := g.SetView("status", -1, -1, maxX, 1); err != nil {
		if err != gocui.ErrorUnkView {
			return err
		}
		v.FgColor = gocui.ColorWhite
		v.BgColor = gocui.ColorBlack
	}

	if v, err := g.SetView("counterHeader", -1, 0, columnX, maxY); err != nil {
		if err != gocui.ErrorUnkView {
			return err
		}
		v.FgColor = gocui.ColorWhite
		v.BgColor = gocui.ColorBlue
		fmt.Fprintf(v, "Counters")
	}
	if _, err := g.SetView("counters", -1, 1, columnX, maxY); err != nil {
		if err != gocui.ErrorUnkView {
			return err
		}
	}

	if v, err := g.SetView("gaugeHeader", columnX, 0, columnX*2, maxY); err != nil {
		if err != gocui.ErrorUnkView {
			return err
		}
		v.FgColor = gocui.ColorWhite
		v.BgColor = gocui.ColorCyan
		fmt.Fprintf(v, "Gauges")
	}
	if _, err := g.SetView("gauges", columnX, 1, columnX*2, maxY); err != nil {
		if err != gocui.ErrorUnkView {
			return err
		}
	}

	if v, err := g.SetView("timerHeader", columnX*2, 0, maxX-1, maxY); err != nil {
		if err != gocui.ErrorUnkView {
			return err
		}
		v.FgColor = gocui.ColorWhite
		v.BgColor = gocui.ColorMagenta
		fmt.Fprintf(v, "Timers")
	}
	if _, err := g.SetView("timers", (columnX*2), 1, maxX-1, maxY); err != nil {
		if err != gocui.ErrorUnkView {
			return err
		}
	}

	return nil
}

func updateViews(g *gocui.Gui) {
	for {
		time.Sleep(1000 * time.Millisecond)

		if sv := g.View("status"); sv != nil {
			sv.Clear()
			fmt.Fprintln(sv, time.Now().Local())
		}

		if sv := g.View("counters"); sv != nil {
			sv.Clear()
			
			indexedSlice := make([]string, len(counters))
			i := 0
			for k, _ := range counters {
				indexedSlice[i] = k
				i++
			}
			sort.Strings(indexedSlice)
			
			for _, value := range indexedSlice {
				fmt.Fprintf(sv, "%s =\t%d\n", value, counters[value].value)
				var duration time.Duration = time.Now().Sub(counters[value].received)
				if duration.Seconds() > float64(*flushInterval) {
					delete(counters, value)
				}
			}
		}

		if sv := g.View("gauges"); sv != nil {
			sv.Clear()

			indexedSlice := make([]string, len(gauges))
			i := 0
			for k, _ := range gauges {
				indexedSlice[i] = k
				i++
			}
			sort.Strings(indexedSlice)

			for _, value := range indexedSlice {
				fmt.Fprintf(sv, "%s =\t%d\n", value, gauges[value])
			}
		}

		if sv := g.View("timers"); sv != nil {
			sv.Clear()

			indexedSlice := make([]string, len(timers))
			i := 0
			for k, _ := range timers {
				indexedSlice[i] = k
				i++
			}
			sort.Strings(indexedSlice)

			for _, value := range indexedSlice {
				calculateAverageTime(sv, value, timers[value]);
			}
		}

		if err := g.Flush(); err != nil {
			return
		}
	}
}

func calculateAverageTime(sv *gocui.View, label string, timeSegments Uint64Slice) {
	var total uint64
	var sum uint64
	
	for index, value := range timeSegments {
		if index < len(timeSegments) - 1 {
			sum = sum + (timeSegments[index + 1] - value)
			total = total + 1
		}
	}

	var avg = float64(0)
	
	if total == 0 {
		avg = 0
	} else {
		avg = float64(sum) / float64(total)
	}

	fmt.Fprintf(sv, "%s =\t%v\n", label, avg)
}

func quit(g *gocui.Gui, v *gocui.View) error {
	return gocui.ErrorQuit
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

	g := gocui.NewGui()
	if err := g.Init(); err != nil {
		log.Panicln(err)
	}
	defer g.Close()

	g.SetLayout(layout)
	if err := g.SetKeybinding("", gocui.KeyCtrlC, 0, quit); err != nil {
		log.Panicln(err)
	}

	go updateViews(g)
	go monitor()

	err := g.MainLoop()
	if err != nil && err != gocui.ErrorQuit {
		log.Panicln(err)
	}
}
