// Command bench measures committed HTTP Put throughput against a running cluster.
//
//	go run ./cmd -dev
//	go run ./cmd/bench
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		base    = flag.String("addr", "http://127.0.0.1:18101", "any node HTTP address")
		seqN    = flag.Int("seq", 500, "sequential Puts")
		concN   = flag.Int("n", 1000, "Puts per concurrency level")
		timeout = flag.Duration("timeout", 8*time.Second, "per-request timeout")
	)
	flag.Parse()

	leader, err := waitLeader(*base, 10*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "no leader: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("leader %s\n\n", leader)

	client := &http.Client{Timeout: *timeout}

	seq := runSequential(client, leader, *seqN)
	printRow("sequential (1 client)", seq)

	fmt.Println()
	for _, c := range []int{4, 8, 16, 32} {
		row := runConcurrent(client, leader, c, *concN)
		printRow(fmt.Sprintf("concurrent %d clients", c), row)
	}
}

type row struct {
	ok    int
	fail  int
	took  time.Duration
	putsS float64
}

func printRow(label string, r row) {
	fmt.Printf("%-24s  %4d ok  %d fail  %.0f puts/sec  (%s)\n",
		label, r.ok, r.fail, r.putsS, r.took.Round(time.Millisecond))
}

func runSequential(client *http.Client, leader string, n int) row {
	start := time.Now()
	ok, fail := 0, 0
	for i := 0; i < n; i++ {
		if err := put(client, leader, fmt.Sprintf("seq-%d", i), "v"); err != nil {
			fail++
			continue
		}
		ok++
	}
	took := time.Since(start)
	return row{ok: ok, fail: fail, took: took, putsS: float64(ok) / took.Seconds()}
}

func runConcurrent(client *http.Client, leader string, conc, n int) row {
	var ok, fail atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	ch := make(chan int, n)
	for i := 0; i < n; i++ {
		ch <- i
	}
	close(ch)
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range ch {
				if err := put(client, leader, fmt.Sprintf("c%d-%d", conc, i), "v"); err != nil {
					fail.Add(1)
					continue
				}
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	took := time.Since(start)
	nOK := int(ok.Load())
	return row{ok: nOK, fail: int(fail.Load()), took: took, putsS: float64(nOK) / took.Seconds()}
}

func put(client *http.Client, leader, key, val string) error {
	req, err := http.NewRequest(http.MethodPut, leader+"/kv/"+key, strings.NewReader(val))
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func waitLeader(base string, d time.Duration) (string, error) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		addr, err := leaderHTTP(base)
		if err == nil && addr != "" {
			return addr, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", fmt.Errorf("timed out waiting for leader at %s", base)
}

func leaderHTTP(base string) (string, error) {
	resp, err := http.Get(base + "/status")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var st struct {
		State      string `json:"state"`
		LeaderHTTP string `json:"leader_http"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return "", err
	}
	if st.State == "leader" {
		return base, nil
	}
	if st.LeaderHTTP == "" {
		return "", fmt.Errorf("no leader yet")
	}
	if strings.HasPrefix(st.LeaderHTTP, "http://") || strings.HasPrefix(st.LeaderHTTP, "https://") {
		return st.LeaderHTTP, nil
	}
	return "http://" + st.LeaderHTTP, nil
}
