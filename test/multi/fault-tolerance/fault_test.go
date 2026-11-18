package faulttolerance

import (
	"fmt"
	"kvStore/proto"
	"strings"
	"testing"
	"time"
)

const writerConns = 8

// TestFollowersDown: kill 0, 1, 2 followers before the load starts. With 3+
// of 5 alive every write must still commit.
func TestFollowersDown(t *testing.T) {
	requireBench(t)
	for _, down := range []int{0, 1, 2} {
		t.Run(fmt.Sprintf("down=%d", down), func(t *testing.T) {
			c := startCluster(t, clusterSize)
			ld := c.leader(5 * time.Second)
			for _, f := range c.followers(ld)[:down] {
				c.kill(f)
			}
			res := writeLoad(c, writerConns, workDur())
			lost := lostAcked(c, res.acks)
			ok := len(res.acks) > 0 && lost == 0
			if !ok {
				t.Errorf("acked=%d lost=%d errs=%d", len(res.acks), lost, res.errs)
			}
			record("Followers down (leader alive)",
				fmt.Sprintf("%d writer conns, %s of unique-key SETs after the kill.", writerConns, workDur()),
				[]string{"down", "alive", "writes/s", "p50 / p99 ms", "errors", "lost", "result"},
				[]string{fmt.Sprint(down), fmt.Sprint(clusterSize - down),
					fmt.Sprintf("%.0f", float64(len(res.acks))/workDur().Seconds()),
					ms(latPct(res.acks, .5)) + " / " + ms(latPct(res.acks, .99)),
					fmt.Sprint(res.errs), fmt.Sprint(lost), passFail(ok)})
		})
	}
}

// TestLeaderDown: kill the leader (alone, or with one follower) in the middle
// of a write load. Clients must find the new leader; the gap is the election.
func TestLeaderDown(t *testing.T) {
	requireBench(t)
	for _, extra := range []int{0, 1} {
		name := "leader"
		if extra == 1 {
			name = "leader + 1 follower"
		}
		t.Run(name, func(t *testing.T) {
			c := startCluster(t, clusterSize)
			ld := c.leader(5 * time.Second)
			victims := append([]int{ld}, c.followers(ld)[:extra]...)

			warm := time.Second
			start := time.Now()
			killAt := start.Add(warm)
			go func() {
				time.Sleep(time.Until(killAt))
				for _, v := range victims {
					c.kill(v)
				}
			}()
			res := writeLoad(c, writerConns, warm+workDur())
			end := time.Now()

			var before, after int
			var firstAfter time.Time
			maxGap, prev := time.Duration(0), start
			for _, a := range res.acks {
				if g := a.at.Sub(prev); g > maxGap {
					maxGap = g
				}
				prev = a.at
				if a.at.Before(killAt) {
					before++
				} else {
					if firstAfter.IsZero() {
						firstAfter = a.at
					}
					after++
				}
			}
			failover := firstAfter.Sub(killAt)
			lost := lostAcked(c, res.acks)
			ok := !firstAfter.IsZero() && lost == 0
			if !ok {
				t.Errorf("no write after kill or lost=%d", lost)
			}
			afterRate := 0.0
			if !firstAfter.IsZero() {
				afterRate = float64(after) / end.Sub(firstAfter).Seconds()
			}
			record("Leader killed mid-load",
				fmt.Sprintf("%d writer conns; kill at t=%s, load runs %s more. "+
					"`failover` = kill -> first acked write (detection + 150-300 ms election + client redirect).",
					writerConns, warm, workDur()),
				[]string{"killed", "writes/s before", "failover ms", "max gap ms", "writes/s after", "client errors", "lost", "result"},
				[]string{name,
					fmt.Sprintf("%.0f", float64(before)/warm.Seconds()),
					ms(failover), ms(maxGap),
					fmt.Sprintf("%.0f", afterRate),
					fmt.Sprint(res.errs), fmt.Sprint(lost), passFail(ok)})
		})
	}
}

// TestQuorumLost: with 3 of 5 down no write may be acked. Reads on survivors
// return the last committed value (server.read.from=any). Restarting one node
// restores the quorum and writes resume.
func TestQuorumLost(t *testing.T) {
	requireBench(t)
	for _, leaderDies := range []bool{false, true} {
		name := "3 followers (leader alive)"
		if leaderDies {
			name = "leader + 2 followers"
		}
		t.Run(name, func(t *testing.T) {
			c := startCluster(t, clusterSize)
			ld := c.leader(5 * time.Second)
			key := []byte("q")
			if r, err := oneShot(c.nodes[ld].clientAddr, proto.Command{Op: proto.OpSet, Key: key, Value: []byte("v1")}, 5*time.Second); err != nil || r.Status != proto.StatusOk {
				t.Fatalf("seed write failed: %v %v", r.Status, err)
			}
			time.Sleep(200 * time.Millisecond) // let followers apply v1

			fs := c.followers(ld)
			victims := fs[:3]
			if leaderDies {
				victims = append([]int{ld}, fs[:2]...)
			}
			for _, v := range victims {
				c.kill(v)
			}
			time.Sleep(time.Second) // give survivors time to (try to) elect

			// A write to every survivor: none may be acked.
			acked, outcomes := 0, ""
			for i, n := range c.nodes {
				if !n.alive() {
					continue
				}
				t0 := time.Now()
				r, err := oneShot(n.clientAddr, proto.Command{Op: proto.OpSet, Key: key, Value: []byte("v2")}, 8*time.Second)
				out := statusName(r.Status, err)
				if err == nil && r.Status == proto.StatusOk {
					acked++
				}
				role := "follower"
				if i == ld {
					role = "leader"
				}
				if outcomes != "" {
					outcomes += "; "
				}
				outcomes += fmt.Sprintf("%s: %s in %s ms", role, out, ms(time.Since(t0)))
			}

			// Reads on survivors still answer from local state.
			reads := ""
			for _, n := range c.nodes {
				if n.alive() {
					r, err := oneShot(n.clientAddr, proto.Command{Op: proto.OpGet, Key: key}, 2*time.Second)
					reads += readName(r, err) + " "
				}
			}

			// Bring one node back: quorum restored, writes should resume.
			t0 := time.Now()
			c.start(victims[len(victims)-1])
			c.leader(15 * time.Second)
			recover := time.Since(t0)
			after, _ := oneShot(c.nodes[c.leader(5*time.Second)].clientAddr, proto.Command{Op: proto.OpGet, Key: key}, 2*time.Second)

			ok := acked == 0
			if !ok {
				t.Errorf("%d writes acked without a quorum", acked)
			}
			record("Quorum lost (3 of 5 down)",
				"One SET q=v2 sent to each survivor (q=v1 committed before the kill). "+
					"Then one killed node restarts from its WAL. "+
					"`q after` may be v1 or v2: a write that timed out on a live leader is *unknown*, not failed, and may commit once the quorum returns.",
				[]string{"killed", "write outcome per survivor", "acked", "reads of q", "restart 1 -> write ok", "q after", "result"},
				[]string{name, outcomes, fmt.Sprint(acked), strings.TrimSpace(reads), ms(recover) + " ms", readName(after, nil), passFail(ok)})
		})
	}
}

func statusName(s byte, err error) string {
	if err != nil {
		return "client timeout/err"
	}
	switch s {
	case proto.StatusOk:
		return "OK"
	case proto.StatusNotLeader:
		return "NotLeader"
	case proto.StatusError:
		return "Error(commit timeout)"
	}
	return fmt.Sprint(s)
}

func readName(r proto.Response, err error) string {
	switch {
	case err != nil:
		return "err"
	case r.Status == proto.StatusOk:
		return string(r.Value)
	case r.Status == proto.StatusNotFound:
		return "missing"
	}
	return statusName(r.Status, nil)
}
