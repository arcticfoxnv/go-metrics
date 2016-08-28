package metrics

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

var shortHostName string = ""

// OpenTSDBConfig provides a container with configuration parameters for
// the OpenTSDB exporter
type OpenTSDBConfig struct {
	Addr          *net.TCPAddr      // Network address to connect to
	Registry      Registry          // Registry to be exported
	FlushInterval time.Duration     // Flush interval
	DurationUnit  time.Duration     // Time conversion unit for durations
	Prefix        string            // Prefix to be prepended to metric names
	Tags          map[string]string // Allows tags to be added in form of key=value
}

// OpenTSDB is a blocking exporter function which reports metrics in r
// to a TSDB server located at addr, flushing them every d duration
// and prepending metric names with prefix.
func OpenTSDB(r Registry, d time.Duration, prefix string, addr *net.TCPAddr, tags map[string]string) {
	OpenTSDBWithConfig(OpenTSDBConfig{
		Addr:          addr,
		Registry:      r,
		FlushInterval: d,
		DurationUnit:  time.Nanosecond,
		Prefix:        prefix,
		Tags:          tags,
	})
}

// OpenTSDBWithConfig is a blocking exporter function just like OpenTSDB,
// but it takes a OpenTSDBConfig instead.
func OpenTSDBWithConfig(c OpenTSDBConfig) {
	for _ = range time.Tick(c.FlushInterval) {
		if err := openTSDB(&c); nil != err {
			log.Println(err)
		}
	}
}

func getShortHostname() string {
	if shortHostName == "" {
		host, _ := os.Hostname()
		if index := strings.Index(host, "."); index > 0 {
			shortHostName = host[:index]
		} else {
			shortHostName = host
		}
	}
	return shortHostName
}

func openTSDB(c *OpenTSDBConfig) error {
	shortHostname := getShortHostname()
	now := time.Now().Unix()
	du := float64(c.DurationUnit)
	conn, err := net.DialTCP("tcp", nil, c.Addr)
	if nil != err {
		return err
	}
	defer conn.Close()

	tagArr := make([]string, len(c.Tags))
	for k, v := range c.Tags {
		tagArr = append(tagArr, fmt.Sprintf("%s=%s", k, v))
	}
	tags := strings.Join(tagArr, " ")

	w := bufio.NewWriter(conn)
	c.Registry.Each(func(name string, i interface{}) {
		switch metric := i.(type) {
		case Counter:
			fmt.Fprintf(w, "put %s.%s.count %d %d host=%s %s\n", c.Prefix, name, now, metric.Count(), shortHostname, tags)
		case Gauge:
			fmt.Fprintf(w, "put %s.%s.value %d %d host=%s %s\n", c.Prefix, name, now, metric.Value(), shortHostname, tags)
		case GaugeFloat64:
			fmt.Fprintf(w, "put %s.%s.value %d %f host=%s %s\n", c.Prefix, name, now, metric.Value(), shortHostname, tags)
		case Histogram:
			h := metric.Snapshot()
			ps := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			fmt.Fprintf(w, "put %s.%s.count %d %d host=%s %s\n", c.Prefix, name, now, h.Count(), shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.min %d %d host=%s %s\n", c.Prefix, name, now, h.Min(), shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.max %d %d host=%s %s\n", c.Prefix, name, now, h.Max(), shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.mean %d %.2f host=%s %s\n", c.Prefix, name, now, h.Mean(), shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.std-dev %d %.2f host=%s %s\n", c.Prefix, name, now, h.StdDev(), shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.50-percentile %d %.2f host=%s %s\n", c.Prefix, name, now, ps[0], shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.75-percentile %d %.2f host=%s %s\n", c.Prefix, name, now, ps[1], shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.95-percentile %d %.2f host=%s %s\n", c.Prefix, name, now, ps[2], shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.99-percentile %d %.2f host=%s %s\n", c.Prefix, name, now, ps[3], shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.999-percentile %d %.2f host=%s %s\n", c.Prefix, name, now, ps[4], shortHostname, tags)
		case Meter:
			m := metric.Snapshot()
			fmt.Fprintf(w, "put %s.%s.count %d %d host=%s %s\n", c.Prefix, name, now, m.Count(), shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.one-minute %d %.2f host=%s %s\n", c.Prefix, name, now, m.Rate1(), shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.five-minute %d %.2f host=%s %s\n", c.Prefix, name, now, m.Rate5(), shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.fifteen-minute %d %.2f host=%s %s\n", c.Prefix, name, now, m.Rate15(), shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.mean %d %.2f host=%s %s\n", c.Prefix, name, now, m.RateMean(), shortHostname, tags)
		case Timer:
			t := metric.Snapshot()
			ps := t.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			fmt.Fprintf(w, "put %s.%s.count %d %d host=%s %s\n", c.Prefix, name, now, t.Count(), shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.min %d %d host=%s %s\n", c.Prefix, name, now, t.Min()/int64(du), shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.max %d %d host=%s %s\n", c.Prefix, name, now, t.Max()/int64(du), shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.mean %d %.2f host=%s %s\n", c.Prefix, name, now, t.Mean()/du, shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.std-dev %d %.2f host=%s %s\n", c.Prefix, name, now, t.StdDev()/du, shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.50-percentile %d %.2f host=%s %s\n", c.Prefix, name, now, ps[0]/du, shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.75-percentile %d %.2f host=%s %s\n", c.Prefix, name, now, ps[1]/du, shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.95-percentile %d %.2f host=%s %s\n", c.Prefix, name, now, ps[2]/du, shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.99-percentile %d %.2f host=%s %s\n", c.Prefix, name, now, ps[3]/du, shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.999-percentile %d %.2f host=%s %s\n", c.Prefix, name, now, ps[4]/du, shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.one-minute %d %.2f host=%s %s\n", c.Prefix, name, now, t.Rate1(), shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.five-minute %d %.2f host=%s %s\n", c.Prefix, name, now, t.Rate5(), shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.fifteen-minute %d %.2f host=%s %s\n", c.Prefix, name, now, t.Rate15(), shortHostname, tags)
			fmt.Fprintf(w, "put %s.%s.mean-rate %d %.2f host=%s %s\n", c.Prefix, name, now, t.RateMean(), shortHostname, tags)
		}
		w.Flush()
	})
	return nil
}
