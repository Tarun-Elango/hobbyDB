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

// startSeeded starts a node whose WAL already holds `keys` live keys of valSize.
func startSeeded(t *testing.T, valSize int) (*serverProc, int) {
	t.Helper()
	dir := t.TempDir()
	keys := keysFor(valSize)
	if _, err := seedWAL(dir, keys, keys, valSize); err != nil {
		t.Fatal(err)
	}
	return startServer(t, dir), keys
}

func perSec(n int, d time.Duration) float64 { return float64(n) / d.Seconds() }

func p50p99(lats []time.Duration) string {
	if len(lats) == 0 {
		return "-"
	}
	return ms(pct(lats, 0.50)) + " / " + ms(pct(lats, 0.99))
}

// --------------------------------------------------------------- startup

// TestStartup measures restart time against WAL length. The log is a churned
// history (see seedWAL), so a 1M-record WAL holds ~90k live keys: replay cost
// follows log length, not DB size, because nothing compacts the log.
func TestStartup(t *testing.T) {
	requireBench(t)
	sizes := envInts("KV_BENCH_WAL_SIZES", []int{0, 100_000, 1_000_000})
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
			med := func(d []time.Duration) time.Duration {
				sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
				return d[len(d)/2]
			}
			record("Startup vs WAL length",
				fmt.Sprintf("Median of %d restarts. `listen` = WAL replayed, port open. `ready` = first write acked "+
					"(adds the 150-300 ms election + re-apply). Churned log: live keys << records.", reps),
				[]string{"WAL records", "WAL MiB", "live keys", "listen ms", "ready ms", "RSS MiB"},
				[]string{fmt.Sprint(n), fmt.Sprintf("%.1f", float64(walSize)/(1<<20)), fmt.Sprint(live),
					ms(med(listen)), ms(med(ready)), fmt.Sprintf("%.0f", rss)})
			if n == sizes[len(sizes)-1] {
				summarize(90, fmt.Sprintf("Restart -> ready, %d-record WAL", n),
					fmt.Sprintf("%s ms, RSS %.0f MiB", ms(med(ready)), rss))
			}
		})
	}
}

// --------------------------------------------------------------- writes

// TestWriteThroughput: 95% SET / 5% DEL over a fixed pre-filled keyspace.
// The 128 B rows sweep connections (does throughput scale = group commit?);
// the 16-conn rows sweep payload (is it fsync- or bandwidth-bound?).
func TestWriteThroughput(t *testing.T) {
	requireBench(t)
	cases := []struct{ size, conns int }{
		{128, 1}, {128, 16}, {128, 64},
		{4096, 16}, {65536, 16},
	}
	for _, cs := range cases {
		t.Run(fmt.Sprintf("value_%dB_conns_%d", cs.size, cs.conns), func(t *testing.T) {
			p, keys := startSeeded(t, cs.size)
			defer p.stop()
			res := runWorkload(t, p.addr, cs.conns, benchDuration(), keys, cs.size, mix{delPct: 5})
			if res.errs > 0 {
				t.Fatalf("%d request errors, last: %s", res.errs, res.lastErr)
			}
			ops := perSec(res.writes.count, res.elapsed)
			record("Writes (SET/DEL 95/5)",
				"Synchronous clients, one request in flight per connection. MiB/s = SET value bytes.",
				[]string{"value", "conns", "ops/s", "MiB/s", "p50 / p99 ms"},
				[]string{humanBytes(cs.size), fmt.Sprint(cs.conns), fmt.Sprintf("%.0f", ops),
					fmt.Sprintf("%.2f", float64(res.valueB)/(1<<20)/res.elapsed.Seconds()),
					p50p99(res.writes.lats)})
			switch {
			case cs.size == 128 && cs.conns == 1:
				summarize(10, "Write, 1 conn (per-op fsync cost)",
					fmt.Sprintf("%.0f ops/s, p50 / p99 %s ms", ops, p50p99(res.writes.lats)))
			case cs.size == 128 && cs.conns == 64:
				summarize(20, "Write, 64 conns (concurrency scaling)",
					fmt.Sprintf("%.0f ops/s, p50 / p99 %s ms", ops, p50p99(res.writes.lats)))
			case cs.size == 65536:
				summarize(30, "Write bandwidth, 64 KiB values",
					fmt.Sprintf("%.1f MiB/s", float64(res.valueB)/(1<<20)/res.elapsed.Seconds()))
			}
		})
	}
}

// --------------------------------------------------------------- reads

// TestReads: dedicated reader connections run flat out while 0 / 1 / 16
// dedicated writer connections write. Shows peak read rate and whether a
// write stream slows reads on OTHER connections.
func TestReads(t *testing.T) {
	requireBench(t)
	const readers, size = 32, 128
	for _, writers := range []int{0, 1, 16} {
		t.Run(fmt.Sprintf("writers_%d", writers), func(t *testing.T) {
			p, keys := startSeeded(t, size)
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
			reads := perSec(rres.reads.count, rres.elapsed)
			wps := "-"
			if writers > 0 {
				wps = fmt.Sprintf("%.0f", perSec(wres.writes.count, wres.elapsed))
			}
			record("Reads under write load (128 B, 32 reader conns)",
				"Readers only GET, writers only SET/DEL, separate connections, same keyspace, at the same time.",
				[]string{"writer conns", "reads/s", "read p50 / p99 ms", "writes/s", "write p50 / p99 ms"},
				[]string{fmt.Sprint(writers), fmt.Sprintf("%.0f", reads), p50p99(rres.reads.lats),
					wps, p50p99(wres.writes.lats)})
			switch writers {
			case 0:
				summarize(40, "Read, 32 conns, no writers",
					fmt.Sprintf("%.0f ops/s, p50 / p99 %s ms", reads, p50p99(rres.reads.lats)))
			case 16:
				summarize(50, "Read, 32 conns, 16 writers active",
					fmt.Sprintf("%.0f ops/s, p50 / p99 %s ms", reads, p50p99(rres.reads.lats)))
			}
		})
	}
}

// --------------------------------------------------------------- mixed

// TestMixed: every connection interleaves GET and SET/DEL (closed loop), the
// shape of a typical app. Throughput here is set by how long each connection
// waits on its own writes.
func TestMixed(t *testing.T) {
	requireBench(t)
	const conns, size = 32, 128
	for _, readPct := range []int{95, 50} {
		t.Run(fmt.Sprintf("read%d", readPct), func(t *testing.T) {
			p, keys := startSeeded(t, size)
			defer p.stop()
			writePct := 100 - readPct
			res := runWorkload(t, p.addr, conns, benchDuration(), keys, size,
				mix{readPct: readPct, delPct: writePct / 10})
			if res.errs > 0 {
				t.Fatalf("%d request errors, last: %s", res.errs, res.lastErr)
			}
			total := perSec(res.total(), res.elapsed)
			record("Mixed (128 B, 32 conns, GET + SET/DEL on each conn)",
				"Closed loop: a connection's next GET waits for its previous write, so write latency caps total ops/s.",
				[]string{"read/write", "total ops/s", "reads/s", "writes/s", "read p50 / p99 ms", "write p50 / p99 ms"},
				[]string{fmt.Sprintf("%d/%d", readPct, writePct), fmt.Sprintf("%.0f", total),
					fmt.Sprintf("%.0f", perSec(res.reads.count, res.elapsed)),
					fmt.Sprintf("%.0f", perSec(res.writes.count, res.elapsed)),
					p50p99(res.reads.lats), p50p99(res.writes.lats)})
			if readPct == 95 {
				summarize(60, "Mixed 95/5, 32 conns", fmt.Sprintf("%.0f ops/s", total))
			}
		})
	}
}
