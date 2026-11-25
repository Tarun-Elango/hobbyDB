// What this file checks:
// - Scaling write and read throughput/latency across cluster sizes (1, 3, and 5 nodes)
// - Mixed read/write workloads (90/10 and 50/50) on a 5-node cluster
// - Concurrency scaling from 1 to 64 client connections (write-only and read-only) on 5 nodes
// - Write throughput and latency impact when 0 to 3 followers have added network latency

package perf

import (
	"fmt"
	"testing"
	"time"
)

// TestScaling: same load on n = 1, 3, 5. Writes pay for a quorum of fsyncs;
// reads can be spread over every node.
func TestScaling(t *testing.T) {
	requireBench(t)
	const conns = 16
	for _, n := range []int{1, 3, 5} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			c := startCluster(t, n, false)
			c.preload()
			w := load(c, conns, 0, workDur())
			r := load(c, conns, 1, workDur())
			record("Scaling with cluster size",
				fmt.Sprintf("%d conns. Write-only run, then read-only run.", conns),
				[]string{"nodes", "writes/s", "write lat", "redirects", "reads/s", "read lat", "errors"},
				[]string{fmt.Sprint(n), rate(w.writeRate()), pcts(w.writes), fmt.Sprint(w.redirects),
					rate(r.readRate()), pcts(r.reads), fmt.Sprint(w.errs + r.errs)})
		})
	}
}

// TestMixed: read/write mixes on 5 nodes, each conn does both.
func TestMixed(t *testing.T) {
	requireBench(t)
	const conns = 32
	c := startCluster(t, 5, false)
	c.preload()
	for _, mix := range []struct {
		name string
		read float64
	}{{"90/10", .9}, {"50/50", .5}} {
		s := load(c, conns, mix.read, workDur())
		record("Mixed workload (5 nodes)",
			fmt.Sprintf("%d conns, each picks GET or SET per op. A conn's next read waits for its previous write, so write latency caps the read rate.", conns),
			[]string{"read/write", "total ops/s", "reads/s", "read lat", "writes/s", "write lat", "redirects", "errors"},
			[]string{mix.name, rate(s.readRate() + s.writeRate()), rate(s.readRate()), pcts(s.reads),
				rate(s.writeRate()), pcts(s.writes), fmt.Sprint(s.redirects), fmt.Sprint(s.errs)})
	}
}

// TestConcurrency: 1..64 clients on 5 nodes, write-only and read-only.
func TestConcurrency(t *testing.T) {
	requireBench(t)
	c := startCluster(t, 5, false)
	c.preload()
	for _, conns := range []int{1, 8, 16, 32, 64} {
		w := load(c, conns, 0, workDur())
		r := load(c, conns, 1, workDur())
		record("Concurrency sweep (5 nodes)", "Write-only run, then read-only run. `redirects` = NotLeader replies to writes, i.e. the leader moved.",
			[]string{"conns", "writes/s", "write lat", "redirects", "reads/s", "read lat", "errors"},
			[]string{fmt.Sprint(conns), rate(w.writeRate()), pcts(w.writes), fmt.Sprint(w.redirects),
				rate(r.readRate()), pcts(r.reads), fmt.Sprint(w.errs + r.errs)})
	}
}

// TestSlowFollower: add one-way latency to the leader's links to 0..3 of its 4
// followers. A write needs the leader + 2 followers, so 1-2 slow followers
// should not matter; 3 slow forces every commit through a slow link.
func TestSlowFollower(t *testing.T) {
	requireBench(t)
	const (
		conns  = 8
		oneWay = 10 * time.Millisecond
	)
	c := startCluster(t, 5, true)
	c.preload()
	for _, slow := range []int{0, 1, 2, 3} {
		ld := c.leader(5 * time.Second)
		fs := c.followers(ld)
		c.clearSlowLinks()
		c.slowLinks(ld, fs[:slow], oneWay)
		time.Sleep(200 * time.Millisecond)
		s := load(c, conns, 0, workDur())
		record("Slow followers (5 nodes)",
			fmt.Sprintf("%d writer conns. Each slow follower gets +%s each way on its link to the leader (+%s RTT). All peer links go through a test proxy.",
				conns, oneWay, 2*oneWay),
			[]string{"slow followers", "writes/s", "write lat", "redirects", "errors"},
			[]string{fmt.Sprint(slow), rate(s.writeRate()), pcts(s.writes), fmt.Sprint(s.redirects), fmt.Sprint(s.errs)})
	}
}
