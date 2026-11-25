package correctness

import (
	"fmt"
	"kvStore/proto"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

// ------------------------------------------------------------ 1. linearizability

// TestLinearizable runs 6 clients doing random GET/SET on 4 hot keys against
// random nodes while the leader is crashed twice, then checks every key's
// history. read.from=leader must be linearizable; read.from=any is
// expected to serve stale follower reads, and the test reports how many.
func TestLinearizable(t *testing.T) {
	requireBench(t)
	for _, mode := range []string{"leader", "any"} {
		t.Run("read.from="+mode, func(t *testing.T) {
			c := startCluster(t, clusterSize, mode)
			const workers, nkeys = 6, 4
			dur := 6 * time.Second
			start := time.Now()
			since := func() int64 { return int64(time.Since(start)) }

			// Faults: kill leader at 1s, restart at 2.5s, kill the new leader at 3.5s, restart at 5s.
			faultDone := make(chan struct{})
			go func() {
				defer close(faultDone)
				var victims []int
				for _, at := range []time.Duration{1 * time.Second, 2500 * time.Millisecond, 3500 * time.Millisecond, 5 * time.Second} {
					time.Sleep(time.Until(start.Add(at)))
					if len(victims) == 0 || at == 3500*time.Millisecond {
						v, ok := c.findLeader(3 * time.Second)
						if !ok {
							t.Errorf("t=%s no leader to kill", at)
							return
						}
						c.kill(v)
						victims = append(victims, v)
						t.Logf("t=%s killed leader %s", at, c.nodes[v].id)
					} else {
						v := victims[len(victims)-1]
						c.start(v)
						t.Logf("t=%s restarted %s", at, c.nodes[v].id)
					}
				}
			}()

			hist := make([][]op, nkeys)
			var mu sync.Mutex
			var wg sync.WaitGroup
			for g := 0; g < workers; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					rng := rand.New(rand.NewSource(int64(g)))
					clientID, seq := uint64(1000+g), uint64(0)
					for time.Since(start) < dur {
						k := rng.Intn(nkeys)
						key := []byte("k" + strconv.Itoa(k))
						node := rng.Intn(clusterSize)
						o := op{call: since()}
						if rng.Intn(2) == 0 {
							seq++
							o.write, o.val = true, fmt.Sprintf("c%d-%d", g, seq)
							_, ok := c.call(node, proto.Command{Op: proto.OpSet, Key: key, Value: []byte(o.val), ClientID: clientID, Seq: seq}, 6*time.Second)
							o.ret = never
							if ok {
								o.ret = since()
							}
						} else {
							resp, ok := c.call(node, proto.Command{Op: proto.OpGet, Key: key}, 6*time.Second)
							if !ok {
								continue // a read with no answer tells us nothing
							}
							o.ret = since()
							if resp.Status == proto.StatusOk {
								o.val = string(resp.Value)
							}
						}
						mu.Lock()
						hist[k] = append(hist[k], o)
						mu.Unlock()
						time.Sleep(time.Duration(5+rng.Intn(20)) * time.Millisecond)
					}
				}(g)
			}
			wg.Wait()
			<-faultDone

			total, stale, bad := 0, 0, 0
			for k, ops := range hist {
				res := checkRegister(ops, 5_000_000)
				s := staleReads(ops)
				total += len(ops)
				stale += s
				if res != linearizable {
					bad++
				}
				t.Logf("key k%d: %d ops, %s, %d provably stale reads", k, len(ops), res, s)
			}
			t.Logf("SUMMARY mode=%s ops=%d keys_not_linearizable=%d/%d stale_reads=%d", mode, total, bad, nkeys, stale)
			status := passFail(bad == 0)
			if mode == "any" {
				status = "expected stale"
				if bad == 0 {
					status = "PASS"
				}
			}
			record("Linearizability under leader crashes",
				"6 clients doing random GET/SET on 4 hot keys for 6s while the leader is SIGKILLed and restarted twice. `read.from=leader` must be linearizable; `read.from=any` is expected to serve stale follower reads.",
				[]string{"read.from", "ops", "linearizable keys", "provably stale reads", "result"},
				[]string{mode, fmt.Sprint(total), fmt.Sprintf("%d/%d", nkeys-bad, nkeys), fmt.Sprint(stale), status})
			if mode == "leader" && bad > 0 {
				t.Errorf("read.from=leader produced a non-linearizable history")
			}
		})
	}
}

// ------------------------------------------------------------ 2. no lost acked writes

// TestNoLostAckedWrites writes unique keys from 8 clients while random nodes
// (leader included, never more than 2 at once) are crashed and restarted,
// then crashes the WHOLE cluster and restarts it from the WALs. Every acked
// key must be readable from the leader, and every node must converge to it.
func TestNoLostAckedWrites(t *testing.T) {
	requireBench(t)
	c := startCluster(t, clusterSize, "any")
	dur := 6 * time.Second
	start := time.Now()

	stop := make(chan struct{})
	chaosDone := make(chan struct{})
	faults := 0
	go func() { // chaos: every 600 ms kill a live node (if <2 down) or restart a dead one
		defer close(chaosDone)
		rng := rand.New(rand.NewSource(42))
		var down []int
		for {
			select {
			case <-stop:
				return
			case <-time.After(600 * time.Millisecond):
			}
			if len(down) < 2 && rng.Intn(3) != 0 {
				live := c.live()
				v := live[rng.Intn(len(live))]
				c.kill(v)
				down = append(down, v)
			} else if len(down) > 0 {
				c.start(down[0])
				down = down[1:]
			}
			faults++
		}
	}()

	var mu sync.Mutex
	var acked []string
	unknown := 0
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for seq := uint64(1); time.Since(start) < dur; seq++ {
				key := fmt.Sprintf("w%d-%d", g, seq)
				_, ok := c.call(g%clusterSize, proto.Command{Op: proto.OpSet, Key: []byte(key), Value: []byte(key), ClientID: uint64(g + 1), Seq: seq}, 5*time.Second)
				mu.Lock()
				if ok {
					acked = append(acked, key)
				} else {
					unknown++
				}
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	close(stop)
	<-chaosDone

	// Full-cluster crash and cold restart from the WALs.
	c.killAll()
	t0 := time.Now()
	for i := range c.nodes {
		c.start(i)
	}
	ld := c.leader(15 * time.Second)
	recovery := time.Since(t0)

	cl, err := dial(c.nodes[ld].clientAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.close()
	lost := 0
	for _, k := range acked {
		r, err := cl.do(proto.Command{Op: proto.OpGet, Key: []byte(k)}, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != proto.StatusOk || string(r.Value) != k {
			lost++
		}
	}

	// Convergence: every node's local store must eventually hold every acked key.
	converged, lagging := 0, ""
	for i, n := range c.nodes {
		var missing int
		for deadline := time.Now().Add(5 * time.Second); ; {
			missing = 0
			ncl, err := dial(n.clientAddr)
			if err == nil {
				for _, k := range acked {
					r, err := ncl.do(proto.Command{Op: proto.OpGet, Key: []byte(k)}, 2*time.Second)
					if err != nil || r.Status != proto.StatusOk || string(r.Value) != k {
						missing++
					}
				}
				ncl.close()
			}
			if (err == nil && missing == 0) || time.Now().After(deadline) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if missing == 0 {
			converged++
		} else {
			lagging += fmt.Sprintf(" %s(missing %d)", c.nodes[i].id, missing)
		}
	}

	t.Logf("SUMMARY acked=%d unknown=%d faults=%d full_restart_to_leader=%s lost=%d converged=%d/%d%s",
		len(acked), unknown, faults, recovery.Round(time.Millisecond), lost, converged, clusterSize, lagging)
	ok := len(acked) > 0 && lost == 0 && converged == clusterSize
	record("No lost acked writes",
		"8 writers for 6s under random node crashes (<=2 down at once), followed by a full-cluster SIGKILL and cold restart from WALs.",
		[]string{"acked", "unknown", "faults", "restart -> leader", "lost", "converged", "result"},
		[]string{fmt.Sprint(len(acked)), fmt.Sprint(unknown), fmt.Sprint(faults),
			recovery.Round(time.Millisecond).String(), fmt.Sprint(lost),
			fmt.Sprintf("%d/%d", converged, clusterSize), passFail(ok)})
	if len(acked) == 0 {
		t.Fatal("no writes acked")
	}
	if lost > 0 {
		t.Errorf("%d acked writes lost", lost)
	}
	if converged != clusterSize {
		t.Errorf("replicas did not converge:%s", lagging)
	}
}

// ------------------------------------------------------------ 3. stale reads

// TestStaleReads measures stale reads in three situations:
//
//   - read-your-writes: SET k=i acked by the leader, then GET k immediately on
//     every follower.
//   - monotonic reads: one client reads k from a random node each time while a
//     writer keeps increasing it; count times the value went backwards.
//   - paused old leader: SIGSTOP the leader, let a new one take over and commit
//     k=new, then send GET k to the old leader and SIGCONT it.
func TestStaleReads(t *testing.T) {
	requireBench(t)
	for _, mode := range []string{"leader", "any"} {
		t.Run("read.from="+mode, func(t *testing.T) {
			c := startCluster(t, clusterSize, mode)
			key := []byte("ryw")

			// read-your-writes
			const rounds = 100
			stale, reads := 0, 0
			for i := 1; i <= rounds; i++ {
				ld := c.leader(5 * time.Second)
				want := strconv.Itoa(i)
				if _, ok := c.call(ld, proto.Command{Op: proto.OpSet, Key: key, Value: []byte(want)}, 5*time.Second); !ok {
					t.Fatalf("write %d not acked", i)
				}
				for j := range c.nodes {
					if j == ld {
						continue
					}
					r, ok := c.call(j, proto.Command{Op: proto.OpGet, Key: key}, 5*time.Second)
					reads++
					if !ok || string(r.Value) != want {
						stale++
					}
				}
			}
			t.Logf("SUMMARY mode=%s read-your-writes: %d/%d follower-started reads stale (%.0f%%)", mode, stale, reads, 100*float64(stale)/float64(reads))
			if mode == "leader" && stale > 0 {
				t.Errorf("read.from=leader returned %d stale reads", stale)
			}

			// monotonic reads
			mkey := []byte("mono")
			mld := c.leader(5 * time.Second)
			stopW := make(chan struct{})
			writerDone := make(chan struct{})
			go func() {
				defer close(writerDone)
				for i := 1; ; i++ {
					select {
					case <-stopW:
						return
					default:
					}
					c.call(mld, proto.Command{Op: proto.OpSet, Key: mkey, Value: []byte(strconv.Itoa(i))}, 5*time.Second)
				}
			}()
			rng := rand.New(rand.NewSource(1))
			last, back, mreads := 0, 0, 0
			for end := time.Now().Add(3 * time.Second); time.Now().Before(end); {
				r, ok := c.call(rng.Intn(clusterSize), proto.Command{Op: proto.OpGet, Key: mkey}, 5*time.Second)
				if !ok {
					continue
				}
				v, _ := strconv.Atoi(string(r.Value))
				mreads++
				if v < last {
					back++
				}
				if v > last {
					last = v
				}
			}
			close(stopW)
			<-writerDone
			t.Logf("SUMMARY mode=%s monotonic reads: value went backwards %d/%d reads", mode, back, mreads)
			if mode == "leader" && back > 0 {
				t.Errorf("read.from=leader: value went backwards %d times", back)
			}

			// paused old leader
			pkey := []byte("paused")
			old := c.leader(5 * time.Second)
			if _, ok := c.call(old, proto.Command{Op: proto.OpSet, Key: pkey, Value: []byte("old")}, 5*time.Second); !ok {
				t.Fatal("seed write failed")
			}
			time.Sleep(200 * time.Millisecond) // followers apply "old"
			stuck, err := dial(c.nodes[old].clientAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer stuck.close()
			c.pause(old)
			nl := c.leader(10 * time.Second)
			if _, ok := c.call(nl, proto.Command{Op: proto.OpSet, Key: pkey, Value: []byte("new")}, 5*time.Second); !ok {
				t.Fatal("write on new leader failed")
			}
			// Queue the GET while the old leader is frozen, then wake it up.
			_ = stuck.conn.SetDeadline(time.Now().Add(5 * time.Second))
			if err := stuck.send(proto.Command{Op: proto.OpGet, Key: pkey}); err != nil {
				t.Fatal(err)
			}
			c.resume(old)
			r, err := proto.DecodeResponse(stuck.r)
			got := "err"
			switch {
			case err != nil:
			case r.Status == proto.StatusOk:
				got = string(r.Value)
			case r.Status == proto.StatusNotLeader:
				got = "NotLeader"
			default:
				got = fmt.Sprintf("status %d", r.Status)
			}
			t.Logf("SUMMARY mode=%s paused old leader %s -> %s, new leader %s committed k=new; GET on old leader returned %q",
				mode, c.nodes[old].id, "resumed", c.nodes[nl].id, got)
			pausedOK := got != "old"
			rywRes := passFail(stale == 0)
			monoRes := passFail(back == 0)
			pausedRes := passFail(pausedOK)
			if mode == "any" {
				rywRes = "info"
				monoRes = "info"
				pausedRes = "info"
			}
			record("Stale reads",
				"Read-your-writes (100 leader SETs then GET on each follower), monotonic reads (random node GETs over 3s), and stalled old leader (SIGSTOP, new leader commits `new`, GET on old leader, SIGCONT).",
				[]string{"read.from", "read-your-writes stale", "monotonic backwards", "paused old leader GET", "result"},
				[]string{mode,
					fmt.Sprintf("%d/%d (%.0f%%)", stale, reads, 100*float64(stale)/float64(reads)),
					fmt.Sprintf("%d/%d", back, mreads),
					got,
					fmt.Sprintf("ryw:%s / mono:%s / paused:%s", rywRes, monoRes, pausedRes)})
			if mode == "leader" && got == "old" {
				t.Errorf("read.from=leader: deposed leader served stale value %q (no leadership check before reads)", got)
			}
		})
	}
}
