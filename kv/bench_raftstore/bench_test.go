package bench_raftstore

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/test_raftstore"
)

func BenchmarkReadWrite(b *testing.B) {
	nservers := 3
	cfg := config.NewTestConfig()
	cfg.RaftLogGcCountLimit = uint64(100)
	cfg.RegionMaxSize = 300
	cfg.RegionSplitSize = 200
	cluster := test_raftstore.NewTestCluster(nservers, cfg)
	cluster.Start()
	defer cluster.Shutdown()

	electionTimeout := cfg.RaftBaseTickInterval * time.Duration(cfg.RaftElectionTimeoutTicks)
	// Wait for leader election
	time.Sleep(2 * electionTimeout)

	nclients := 16
	chTasks := make(chan int, 1000)
	clnts := make([]chan bool, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan bool, 1)
		go func(cli int) {
			defer func() {
				clnts[cli] <- true
			}()
			for {
				j, more := <-chTasks
				if more {
					key := fmt.Sprintf("%02d%08d", cli, j)
					value := "x " + strconv.Itoa(j) + " y"
					cluster.MustPut([]byte(key), []byte(value))
					if (rand.Int() % 1000) < 500 {
						value := cluster.Get([]byte(key))
						if value == nil {
							b.Fatal("value is emtpy")
						}
					}
				} else {
					return
				}
			}
		}(i)
	}

	start := time.Now()
	duration := 1 * time.Minute
	totalRequests := 0
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(start)
			if elapsed >= duration {
				close(chTasks)
				for cli := 0; cli < nclients; cli++ {
					ok := <-clnts[cli]
					if !ok {
						b.Fatalf("failure")
					}
				}
				ticker.Stop()
				totalDuration := time.Since(start)
				b.Logf("Total Duration: %v, Total Requests: %v", totalDuration, totalRequests)
				b.Logf("QPS: %v", float64(totalRequests)/totalDuration.Seconds())
				return
			}
		case chTasks <- totalRequests:
			totalRequests++
		}
	}
}
