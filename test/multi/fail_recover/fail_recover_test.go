package failrecover

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestLeaderElection: kill the leader 5 times (no load) and time how long until
// some node acks a write again. Each killed node is restarted before the next round.
func TestLeaderElection(t *testing.T) {
	requireBench(t)
	c := startCluster(t, clusterSize)
	var times []time.Duration
	for round := 0; round < 5; round++ {
		ld := c.leader(10 * time.Second)
		c.kill(ld)
		t0 := time.Now()
		nl := c.leader(10 * time.Second)
		times = append(times, time.Since(t0))
		if nl == ld {
			t.Fatalf("killed leader %d still leader", ld)
		}
		c.start(ld)
		if _, ok := waitConverged(c, 10*time.Second); !ok {
			t.Fatal("cluster did not converge after restart")
		}
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	record("Leader election",
		"Leader SIGKILLed 5 times, no load. Time = kill -> first acked write on the new leader. Election timeout is 150-300 ms.",
		[]string{"rounds", "min ms", "median ms", "max ms", "result"},
		[]string{"5", ms(times[0]), ms(times[2]), ms(times[4]), passFail(times[4] < 2*time.Second)})
}

// TestRejoinCatchUp: a follower is down while the cluster takes writes, then
// comes back. With its WAL it replays and fetches the rest; with a wiped disk
// it must get the whole log from the leader.
func TestRejoinCatchUp(t *testing.T) {
	requireBench(t)
	for _, wipe := range []bool{false, true} {
		name := "WAL intact"
		if wipe {
			name = "disk wiped"
		}
		t.Run(name, func(t *testing.T) {
			c := startCluster(t, clusterSize)
			// some writes it already has, so "WAL intact" has something to replay
			pre := writeLoad(c, writerConns, time.Second)
			ld := c.leader(5 * time.Second)
			f := c.followers(ld)[0]
			c.kill(f)
			if wipe {
				files, _ := filepath.Glob(c.nodes[f].wal + "*")
				for _, p := range files {
					_ = os.Remove(p)
				}
			}
			res := writeLoad(c, writerConns, 3*time.Second)
			behind := len(res.acks)

			t0 := time.Now()
			c.start(f)
			_, ok := waitConverged(c, 30*time.Second)
			catch := time.Since(t0)
			all := append(pre.acks, res.acks...)
			lost := lostOn(c, f, all)
			ok = ok && lost == 0
			if !ok {
				t.Errorf("converged=%v lost=%d", ok, lost)
			}
			record("Follower rejoin and catch-up",
				"Follower SIGKILLed, cluster takes 3 s of writes, follower restarts. `catch-up` = restart -> its key count matches the leader. `lost` checked on the rejoined node.",
				[]string{"case", "keys missed while down", "total keys", "catch-up ms", "lost", "result"},
				[]string{name, fmt.Sprint(behind), fmt.Sprint(len(all)), ms(catch), fmt.Sprint(lost), passFail(ok)})
		})
	}
}

// TestFullClusterRestart: all 5 nodes are SIGKILLed in the middle of a write
// load, then restarted. Every acked write must come back from the WALs.
func TestFullClusterRestart(t *testing.T) {
	requireBench(t)
	c := startCluster(t, clusterSize)
	killAt := time.Now().Add(2 * time.Second)
	killDone := make(chan struct{})
	go func() {
		defer close(killDone)
		time.Sleep(time.Until(killAt))
		c.killAll()
	}()
	res := writeLoad(c, writerConns, 2500*time.Millisecond)
	<-killDone
	c.killAll()

	t0 := time.Now()
	for i := range c.nodes {
		c.start(i)
	}
	c.leader(15 * time.Second)
	toLeader := time.Since(t0)
	conv, ok := waitConverged(c, 30*time.Second)
	lost := lostAnywhere(c, res.acks)
	ok = ok && lost == 0 && len(res.acks) > 0
	if !ok {
		t.Errorf("acked=%d lost=%d", len(res.acks), lost)
	}
	record("Full-cluster crash and restart",
		"8 writer conns; all 5 nodes SIGKILLed at t=2 s mid-load, then restarted together. `lost` checked on every node.",
		[]string{"acked before crash", "restart -> leader ms", "then converge ms", "lost", "result"},
		[]string{fmt.Sprint(len(res.acks)), ms(toLeader), ms(conv), fmt.Sprint(lost), passFail(ok)})
}

// TestPartitionLeaderMinority: the leader and one follower are cut off from
// the other three. The majority must elect a new leader; the old leader must
// not commit anything; after healing it must adopt the majority's log.
func TestPartitionLeaderMinority(t *testing.T) {
	requireBench(t)
	c := startCluster(t, clusterSize)
	ld := c.leader(5 * time.Second)
	if r, err := set(c, ld, "q", "v1", 5*time.Second); err != nil || statusName(r, err) != "OK" {
		t.Fatalf("seed write: %s", statusName(r, err))
	}
	time.Sleep(200 * time.Millisecond)
	fs := c.followers(ld)
	minority := []int{ld, fs[0]}
	majority := fs[1:]
	c.partition(minority)

	// write on the old leader while it is cut off
	type outcome struct {
		s string
		d time.Duration
	}
	oldW := make(chan outcome, 1)
	go func() {
		t0 := time.Now()
		r, err := set(c, ld, "q", "v2", 8*time.Second)
		oldW <- outcome{statusName(r, err), time.Since(t0)}
	}()

	t0 := time.Now()
	nl := c.leaderAmong(majority, 10*time.Second)
	elect := time.Since(t0)
	r, err := set(c, nl, "q", "v3", 5*time.Second)
	newW := statusName(r, err)
	time.Sleep(100 * time.Millisecond)
	staleRead := get(c, ld, "q")
	old := <-oldW

	c.heal()
	t1 := time.Now()
	for time.Since(t1) < 10*time.Second && get(c, ld, "q") != "v3" {
		time.Sleep(10 * time.Millisecond)
	}
	rejoin := time.Since(t1)
	_, conv := waitConverged(c, 10*time.Second)
	var finals []string
	allV3 := true
	for i := range c.nodes {
		v := get(c, i, "q")
		finals = append(finals, v)
		allV3 = allV3 && v == "v3"
	}
	ok := old.s != "OK" && newW == "OK" && allV3 && conv
	if !ok {
		t.Errorf("old write=%s new write=%s finals=%v converged=%v", old.s, newW, finals, conv)
	}
	record("Partition: old leader in the minority (2 | 3)",
		"q=v1 committed, then {leader, 1 follower} cut off from the other 3. SET q=v2 sent to the old leader, SET q=v3 to the new one. Then heal.",
		[]string{"majority elects ms", "q=v3 on new leader", "q=v2 on old leader", "read q on old leader (cut off)", "heal -> old leader has v3 ms", "q on all nodes after", "result"},
		[]string{ms(elect), newW, fmt.Sprintf("%s after %s ms", old.s, ms(old.d)), staleRead, ms(rejoin), strings.Join(finals, " "), passFail(ok)})
}

// TestPartitionSymmetric: under load, two followers are cut off from the
// leader's side (2 | 3) for 3 s, then healed.
func TestPartitionSymmetric(t *testing.T) {
	requireBench(t)
	c := startCluster(t, clusterSize)
	ld := c.leader(5 * time.Second)
	minority := c.followers(ld)[:2]

	start := time.Now()
	cutAt, healAt, end := start.Add(time.Second), start.Add(4*time.Second), start.Add(7*time.Second)
	partDone := make(chan struct{})
	go func() {
		defer close(partDone)
		time.Sleep(time.Until(cutAt))
		c.partition(minority)
		time.Sleep(time.Until(healAt))
		c.heal()
	}()
	res := writeLoad(c, writerConns, end.Sub(start))
	<-partDone

	after := c.leader(5 * time.Second)
	conv, cok := waitConverged(c, 10*time.Second)
	lost := lostAnywhere(c, res.acks)
	minorityVal := countBetween(res.acks, cutAt, healAt) > 0
	ok := cok && lost == 0 && minorityVal
	if !ok {
		t.Errorf("converged=%v lost=%d writes during partition=%v", cok, lost, minorityVal)
	}
	moved := "no"
	if after != ld {
		moved = "yes"
	}
	record("Partition: symmetric 2 | 3 (leader in the majority)",
		"8 writer conns for 7 s; 2 followers cut off at t=1 s, healed at t=4 s.",
		[]string{"writes/s before", "during", "after heal", "max gap during ms", "max gap after heal ms", "leader changed", "client errors", "converge ms", "lost", "result"},
		[]string{rate(countBetween(res.acks, start, cutAt), time.Second),
			rate(countBetween(res.acks, cutAt, healAt), 3*time.Second),
			rate(countBetween(res.acks, healAt, end), 3*time.Second),
			ms(maxGap(res.acks, cutAt, healAt)), ms(maxGap(res.acks, healAt, end)),
			moved, fmt.Sprint(res.errs), ms(conv), fmt.Sprint(lost), passFail(ok)})
}

// TestFlappingLinks: under load, a random 2 | 3 split is applied for 300 ms,
// healed for 200 ms, over and over for 5 s.
func TestFlappingLinks(t *testing.T) {
	requireBench(t)
	c := startCluster(t, clusterSize)
	rng := rand.New(rand.NewSource(1))
	start := time.Now()
	flapFrom, flapTo, end := start.Add(500*time.Millisecond), start.Add(5500*time.Millisecond), start.Add(7*time.Second)
	flaps := 0
	flapDone := make(chan struct{})
	go func() {
		defer close(flapDone)
		time.Sleep(time.Until(flapFrom))
		for time.Now().Before(flapTo) {
			c.partition(rng.Perm(clusterSize)[:2])
			flaps++
			time.Sleep(300 * time.Millisecond)
			c.heal()
			time.Sleep(200 * time.Millisecond)
		}
	}()
	res := writeLoad(c, writerConns, end.Sub(start))
	<-flapDone
	conv, cok := waitConverged(c, 10*time.Second)
	lost := lostAnywhere(c, res.acks)
	during := countBetween(res.acks, flapFrom, flapTo)
	ok := cok && lost == 0 && during > 0
	if !ok {
		t.Errorf("converged=%v lost=%d writes while flapping=%d", cok, lost, during)
	}
	record("Flapping links",
		"8 writer conns for 7 s; from t=0.5 s to 5.5 s a random 2 | 3 split is on for 300 ms, off for 200 ms (seeded).",
		[]string{"flaps", "writes/s before", "while flapping", "after", "max gap ms", "client errors", "converge ms", "lost", "result"},
		[]string{fmt.Sprint(flaps),
			rate(countBetween(res.acks, start, flapFrom), flapFrom.Sub(start)),
			rate(during, flapTo.Sub(flapFrom)),
			rate(countBetween(res.acks, flapTo, end), end.Sub(flapTo)),
			ms(maxGap(res.acks, flapFrom, flapTo)), fmt.Sprint(res.errs), ms(conv), fmt.Sprint(lost), passFail(ok)})
}

// TestRollingRestart: under load, restart each node in turn (followers first,
// leader last) with a clean SIGTERM, waiting for it to catch up before the next.
func TestRollingRestart(t *testing.T) {
	requireBench(t)
	c := startCluster(t, clusterSize)
	ld := c.leader(5 * time.Second)
	order := append(c.followers(ld), ld)

	start := time.Now()
	rollFrom := start.Add(time.Second)
	var rollTo time.Time
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(time.Until(rollFrom))
		for _, i := range order {
			c.stop(i)
			time.Sleep(300 * time.Millisecond)
			c.start(i)
			time.Sleep(time.Second)
		}
		rollTo = time.Now()
	}()
	res := writeLoad(c, writerConns, 11*time.Second)
	<-done
	conv, cok := waitConverged(c, 10*time.Second)
	lost := lostAnywhere(c, res.acks)
	gap := maxGap(res.acks, rollFrom, rollTo)
	ok := cok && lost == 0 && gap < 2*time.Second
	if !ok {
		t.Errorf("converged=%v lost=%d maxGap=%s", cok, lost, gap)
	}
	record("Rolling restart",
		"8 writer conns for 11 s; each node in turn gets SIGTERM, is down 300 ms, restarts, then 1 s to settle. Followers first, leader last.",
		[]string{"writes/s before", "during roll", "max gap ms", "client errors", "converge ms", "lost", "result"},
		[]string{rate(countBetween(res.acks, start, rollFrom), time.Second),
			rate(countBetween(res.acks, rollFrom, rollTo), rollTo.Sub(rollFrom)),
			ms(gap), fmt.Sprint(res.errs), ms(conv), fmt.Sprint(lost), passFail(ok)})
}
