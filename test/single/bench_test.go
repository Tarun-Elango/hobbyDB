package single

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"kvStore/proto"
)

const ramBudget = 48 << 20 // live data per case; keeps everything memory-resident

// keysFor sizes the keyspace so keys*valSize stays near ramBudget.
func keysFor(valSize int) int {
	k := ramBudget / (valSize + 16)
	if k < 1000 {
		k = 1000
	}
	if k > 50000 {
		k = 50000
	}
	return k
}

// --------------------------------------------------------------- startup

// TestStartup measures restart time against WAL length. The log is a churned
// history (see seedWAL), so a 1M-record WAL holds ~100k live keys: replay cost
// follows log length, not DB size, because nothing compacts the log.
func TestStartup(t *testing.T) {
	requireBench(t)
	sizes := envInts("KV_BENCH_WAL_SIZES", []int{0, 10_000, 100_000, 1_000_000})
	reps := envInt("KV_BENCH_STARTUP_REPS", 3)
	const keys, valSize = 100_000, 128

	for _, n := range sizes {
		t.Run(fmt.Sprintf("wal_%d", n), func(t *testing.T) {
			dir := t.TempDir()
			live := 0
			if n > 0 {
				var err error
				if live, err = seedWAL(dir, n, keys, valSize); err != nil {
					t.Fatal(err)
				}
			}
			var listen, ready []time.Duration
			var rss float64
			var walSize int64
			for i := 0; i < reps; i++ {
				p := startServer(t, dir)
				listen = append(listen, p.ListenTime)
				ready = append(ready, p.ReadyTime)
				if n > 0 {
					c, err := dialClient(p.addr)
					if err != nil {
						t.Fatal(err)
					}
					resp, err := c.do(proto.Command{Op: proto.OpLen})
					c.close()
					want := fmt.Sprint(live + 1) // + __ready__ probe
					if err != nil || string(resp.Value) != want {
						t.Fatalf("after restart len=%q err=%v, want %s live keys", resp.Value, err, want)
					}
				}
				rss, walSize = p.rssMB(), p.walBytes()
				p.stop()
			}
			sort.Slice(listen, func(i, j int) bool { return listen[i] < listen[j] })
			sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })
			med := func(d []time.Duration) time.Duration { return d[len(d)/2] }
			record("Startup time vs WAL length",
				fmt.Sprintf("Median of %d restarts, 128 B values. `listen` = process start -> client port open (WAL replay into memory). "+
					"`ready` = process start -> first write acked (also needs the election timeout of 150-300 ms and re-applying the whole log to the store). "+
					"Records are a churned history (90%% overwrite / 10%% delete over up to 100k keys), so live keys << records.", reps),
				[]string{"WAL records", "WAL size", "live keys", "listen ms", "ready ms", "server RSS MiB"},
				[]string{fmt.Sprint(n), fmt.Sprintf("%.1f MiB", float64(walSize)/(1<<20)), fmt.Sprint(live),
					ms(med(listen)), ms(med(ready)), fmt.Sprintf("%.0f", rss)})
			t.Logf("records=%d listen=%s ready=%s rss=%.0fMiB", n, med(listen), med(ready), rss)
		})
	}
}

// --------------------------------------------------------------- write throughput

// TestWriteThroughput: 95% SET / 5% DEL over a fixed pre-filled keyspace
// (overwrites + delete/recreate, no unbounded growth), per payload size and
// client count.
func TestWriteThroughput(t *testing.T) {
	requireBench(t)
	payloads := envInts("KV_BENCH_PAYLOADS", []int{16, 128, 1024, 4096, 65536})
	conns := envInts("KV_BENCH_WRITE_CONNS", []int{1, 16, 64})
	for _, size := range payloads {
		for _, c := range conns {
			t.Run(fmt.Sprintf("value_%dB_conns_%d", size, c), func(t *testing.T) {
				dir := t.TempDir()
				keys := keysFor(size)
				if _, err := seedWAL(dir, keys, keys, size); err != nil {
					t.Fatal(err)
				}
				p := startServer(t, dir)
				defer p.stop()

				res := runWorkload(t, p.addr, c, benchDuration(), keys, size, mix{readPct: 0, delPct: 5})
				if res.errs > 0 {
					t.Fatalf("%d request errors, last: %s", res.errs, res.lastErr)
				}
				secs := res.elapsed.Seconds()
				record("Write / update throughput by payload size",
					"95% SET / 5% DEL on a pre-filled keyspace (80% of ops hit the hottest 20% of keys). Synchronous clients, one request in flight per connection. "+
						"MiB/s counts value bytes of SETs only.",
					[]string{"value size", "conns", "keys", "ops/s", "MiB/s", "p50 ms", "p99 ms", "WAL growth MiB", "RSS MiB"},
					[]string{humanBytes(size), fmt.Sprint(c), fmt.Sprint(keys),
						fmt.Sprintf("%.0f", float64(res.writes.count)/secs),
						fmt.Sprintf("%.2f", float64(res.valueB)/(1<<20)/secs),
						ms(pct(res.writes.lats, 0.50)), ms(pct(res.writes.lats, 0.99)),
						fmt.Sprintf("%.1f", float64(p.walBytes())/(1<<20)-float64(keys*(size+40))/(1<<20)),
						fmt.Sprintf("%.0f", p.rssMB())})
			})
		}
	}
}

// --------------------------------------------------------------- mixed read/write

// TestMixedThroughput: GET-heavy and balanced mixes on a pre-filled keyspace.
func TestMixedThroughput(t *testing.T) {
	requireBench(t)
	payloads := envInts("KV_BENCH_MIXED_PAYLOADS", []int{128, 4096})
	readPcts := envInts("KV_BENCH_READ_PCTS", []int{100, 95, 80, 50})
	conns := envInt("KV_BENCH_MIXED_CONNS", 32)

	type c struct{ size, readPct, conns int }
	var cases []c
	for _, s := range payloads {
		for _, r := range readPcts {
			cases = append(cases, c{s, r, conns})
		}
	}
	// connection-count sweep on a 95/5 mix at 128 B
	for _, n := range envInts("KV_BENCH_MIXED_CONN_SWEEP", []int{1, 8, 128}) {
		cases = append(cases, c{128, 95, n})
	}

	for _, cs := range cases {
		t.Run(fmt.Sprintf("value_%dB_read%d_conns_%d", cs.size, cs.readPct, cs.conns), func(t *testing.T) {
			dir := t.TempDir()
			keys := keysFor(cs.size)
			if _, err := seedWAL(dir, keys, keys, cs.size); err != nil {
				t.Fatal(err)
			}
			p := startServer(t, dir)
			defer p.stop()

			writePct := 100 - cs.readPct
			res := runWorkload(t, p.addr, cs.conns, benchDuration(), keys, cs.size,
				mix{readPct: cs.readPct, delPct: writePct / 10})
			if res.errs > 0 {
				t.Fatalf("%d request errors, last: %s", res.errs, res.lastErr)
			}
			secs := res.elapsed.Seconds()
			hit := "-"
			if res.reads.count > 0 {
				hit = fmt.Sprintf("%.0f%%", 100*float64(res.hits)/float64(res.reads.count))
			}
			wp50, wp99 := "-", "-"
			if res.writes.count > 0 {
				wp50, wp99 = ms(pct(res.writes.lats, 0.50)), ms(pct(res.writes.lats, 0.99))
			}
			rp50, rp99 := "-", "-"
			if res.reads.count > 0 {
				rp50, rp99 = ms(pct(res.reads.lats, 0.50)), ms(pct(res.reads.lats, 0.99))
			}
			record("Mixed read/write throughput",
				"Pre-filled keyspace, GET vs SET/DEL (writes are ~90% SET / 10% DEL), 80/20 hot-key skew. `read mix` is the GET percentage. "+
					"Reads are served straight from the in-memory store (server.read.from=any); writes go through Raft + fsync.",
				[]string{"value size", "read mix", "conns", "total ops/s", "reads/s", "writes/s", "GET hit rate", "read p50 / p99 ms", "write p50 / p99 ms"},
				[]string{humanBytes(cs.size), fmt.Sprintf("%d/%d", cs.readPct, writePct), fmt.Sprint(cs.conns),
					fmt.Sprintf("%.0f", float64(res.total())/secs),
					fmt.Sprintf("%.0f", float64(res.reads.count)/secs),
					fmt.Sprintf("%.0f", float64(res.writes.count)/secs), hit,
					rp50 + " / " + rp99, wp50 + " / " + wp99})
		})
	}
}

// --------------------------------------------------------------- reads under write load

// TestReadsUnderWriteLoad separates the two client populations: dedicated
// reader connections run flat out while dedicated writer connections keep
// writing. Unlike TestMixedThroughput (where one connection alternates GET
// and SET, so a slow fsync stalls its own reads), this shows how much a
// write stream slows down reads on OTHER connections.
func TestReadsUnderWriteLoad(t *testing.T) {
	requireBench(t)
	readers := envInt("KV_BENCH_READERS", 32)
	for _, size := range envInts("KV_BENCH_MIXED_PAYLOADS", []int{128, 4096}) {
		for _, writers := range envInts("KV_BENCH_WRITERS", []int{0, 1, 16}) {
			t.Run(fmt.Sprintf("value_%dB_writers_%d", size, writers), func(t *testing.T) {
				dir := t.TempDir()
				keys := keysFor(size)
				if _, err := seedWAL(dir, keys, keys, size); err != nil {
					t.Fatal(err)
				}
				p := startServer(t, dir)
				defer p.stop()

				var rres, wres workloadResult
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					rres = runWorkload(t, p.addr, readers, benchDuration(), keys, size, mix{readPct: 100})
				}()
				if writers > 0 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						wres = runWorkload(t, p.addr, writers, benchDuration(), keys, size, mix{delPct: 5})
					}()
				}
				wg.Wait()
				if rres.errs+wres.errs > 0 {
					t.Fatalf("request errors: %s %s", rres.lastErr, wres.lastErr)
				}
				wps, wp50, wp99 := "-", "-", "-"
				if writers > 0 {
					wps = fmt.Sprintf("%.0f", float64(wres.writes.count)/wres.elapsed.Seconds())
					wp50, wp99 = ms(pct(wres.writes.lats, 0.5)), ms(pct(wres.writes.lats, 0.99))
				}
				record("Reads while writers are active (dedicated connections)",
					"Reader connections only GET, writer connections only SET/DEL (95/5), all hitting the same pre-filled keyspace at the same time.",
					[]string{"value size", "reader conns", "writer conns", "reads/s", "read p50 / p99 ms", "writes/s", "write p50 / p99 ms"},
					[]string{humanBytes(size), fmt.Sprint(readers), fmt.Sprint(writers),
						fmt.Sprintf("%.0f", float64(rres.reads.count)/rres.elapsed.Seconds()),
						ms(pct(rres.reads.lats, 0.5)) + " / " + ms(pct(rres.reads.lats, 0.99)),
						wps, wp50 + " / " + wp99})
			})
		}
	}
}
