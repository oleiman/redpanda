// ct-producer is a high-throughput Kafka producer for compaction stress testing.
// It produces key-cycling messages to a single topic until killed, printing
// JSON stats to stdout every second for the Python orchestrator to read.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type stats struct {
	records    atomic.Int64
	bytes      atomic.Int64
	errors     atomic.Int64
	tombstones atomic.Int64
}

type statsLine struct {
	Records    int64   `json:"records"`
	Bytes      int64   `json:"bytes"`
	Errors     int64   `json:"errors"`
	Tombstones int64   `json:"tombstones"`
	BytesPerS  float64 `json:"bytes_per_sec"`
	Buffered   int64   `json:"buffered"`
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Kafka broker addresses (comma-separated)")
	topic := flag.String("topic", "", "Topic to produce to")
	keyPrefix := flag.String("key-prefix", "k", "Key prefix")
	keyCount := flag.Int("key-count", 10000, "Number of unique keys to cycle through")
	msgSize := flag.Int("msg-size", 512, "Message value size in bytes")
	rateLimitBps := flag.Int64("rate-limit", 0, "Rate limit in bytes/sec (0 = unlimited)")
	tombstoneProb := flag.Float64("tombstone-prob", 0.0, "Probability of tombstone (0.0-1.0)")
	saslMechanism := flag.String("sasl-mechanism", "", "SASL mechanism (SCRAM-SHA-256, SCRAM-SHA-512)")
	saslUser := flag.String("sasl-user", "", "SASL username")
	saslPassword := flag.String("sasl-password", "", "SASL password")
	tlsEnabled := flag.Bool("tls", false, "Enable TLS")
	flag.Parse()

	if *topic == "" {
		fmt.Fprintf(os.Stderr, "error: --topic is required\n")
		os.Exit(1)
	}

	// Pre-generate keys
	keys := make([][]byte, *keyCount)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("%s-%d", *keyPrefix, i))
	}

	// Pre-generate value payload
	value := make([]byte, *msgSize)
	rand.Read(value)

	// Build franz-go client options.
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.ProducerBatchMaxBytes(1 * 1024 * 1024),
		kgo.MaxBufferedRecords(50000),
		kgo.ProducerLinger(10 * time.Millisecond),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
		kgo.RecordPartitioner(kgo.UniformBytesPartitioner(64*1024, true, true, nil)),
		// Aggressive retries — BYOC load balancers can reset connections
		// during metadata refresh, causing transient "no partitions available"
		// errors. Unlimited retries with backoff let the producer recover.
		kgo.RetryBackoffFn(func(n int) time.Duration {
			d := time.Duration(n+1) * 500 * time.Millisecond
			if d > 5*time.Second {
				d = 5 * time.Second
			}
			return d
		}),
		kgo.RecordRetries(0), // 0 = unlimited retries
	}

	if *saslMechanism != "" && *saslUser != "" {
		switch strings.ToUpper(*saslMechanism) {
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: *saslUser,
				Pass: *saslPassword,
			}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: *saslUser,
				Pass: *saslPassword,
			}.AsSha512Mechanism()))
		default:
			fmt.Fprintf(os.Stderr, "unsupported SASL mechanism: %s\n", *saslMechanism)
			os.Exit(1)
		}
	}
	if *tlsEnabled {
		opts = append(opts, kgo.DialTLSConfig(nil))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	var s stats
	var lastErrLog atomic.Int64 // unix timestamp of last error log

	// Stats reporter — prints JSON to stdout every second
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		var prevBytes int64
		prevTime := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now()
				curBytes := s.bytes.Load()
				elapsed := now.Sub(prevTime).Seconds()
				bps := float64(curBytes-prevBytes) / elapsed
				prevBytes = curBytes
				prevTime = now

				errCount := s.errors.Load()
				buffered := int64(client.BufferedProduceRecords())
				line := statsLine{
					Records:    s.records.Load(),
					Bytes:      curBytes,
					Errors:     errCount,
					Tombstones: s.tombstones.Load(),
					BytesPerS:  bps,
					Buffered:   buffered,
				}
				data, _ := json.Marshal(line)
				fmt.Println(string(data))

				// Log to stderr if stuck (high buffer, low throughput)
				if bps == 0 && buffered > 0 {
					fmt.Fprintf(os.Stderr, "STALL: buffered=%d errors=%d bps=0\n", buffered, errCount)
				}
			}
		}
	}()

	// Produce loop — updates stats per-record so reporting stays live
	// even when Produce() blocks on backpressure.
	counter := 0
	kc := *keyCount
	tp := *tombstoneProb
	hasTombstones := tp > 0
	rateLimit := *rateLimitBps

	const batchSize = 1000 // smaller batches for more responsive rate limiting
	for ctx.Err() == nil {
		batchStart := time.Now()
		var batchBytes int64

		for i := 0; i < batchSize && ctx.Err() == nil; i++ {
			key := keys[counter%kc]
			counter++

			var val []byte
			var msgBytes int64
			isTombstone := hasTombstones && mrand.Float64() < tp
			if isTombstone {
				val = nil
				msgBytes = int64(len(key))
				s.tombstones.Add(1)
			} else {
				val = value
				msgBytes = int64(len(key) + len(value))
			}

			// Update stats before Produce so they stay live during backpressure
			s.records.Add(1)
			s.bytes.Add(msgBytes)
			batchBytes += msgBytes

			client.Produce(ctx, &kgo.Record{
				Key:   key,
				Value: val,
			}, func(_ *kgo.Record, err error) {
				if err != nil {
					s.errors.Add(1)
					// Log errors at most once per 5 seconds
					now := time.Now().Unix()
					prev := lastErrLog.Load()
					if now-prev >= 5 && lastErrLog.CompareAndSwap(prev, now) {
						fmt.Fprintf(os.Stderr, "produce error (total=%d): %v\n",
							s.errors.Load(), err)
					}
				}
			})
		}

		// Rate limiting
		if rateLimit > 0 && batchBytes > 0 {
			elapsed := time.Since(batchStart)
			expected := time.Duration(float64(batchBytes) / float64(rateLimit) * float64(time.Second))
			if elapsed < expected {
				sleepFor := expected - elapsed
				if sleepFor > 100*time.Millisecond {
					timer := time.NewTimer(sleepFor)
					select {
					case <-ctx.Done():
						timer.Stop()
					case <-timer.C:
					}
				} else {
					time.Sleep(sleepFor)
				}
			}
		}
	}

	// Flush remaining
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer flushCancel()
	if err := client.Flush(flushCtx); err != nil {
		fmt.Fprintf(os.Stderr, "flush error: %v\n", err)
	}

	// Final stats
	line := statsLine{
		Records:    s.records.Load(),
		Bytes:      s.bytes.Load(),
		Errors:     s.errors.Load(),
		Tombstones: s.tombstones.Load(),
		BytesPerS:  0,
		Buffered:   int64(client.BufferedProduceRecords()),
	}
	data, _ := json.Marshal(line)
	fmt.Println(string(data))
}
