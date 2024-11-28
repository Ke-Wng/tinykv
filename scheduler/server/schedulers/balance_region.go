// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// select all suitable stores
	allStores := cluster.GetStores()
	if len(allStores) < 2 {
		return nil
	}
	suitableStores := make([]*core.StoreInfo, 0)
	for _, store := range allStores {
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime(){
			suitableStores = append(suitableStores, store)
		}
	}
	// sort them according to their region size
	sort.Slice(suitableStores, func(i,j int) bool{
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})

	// find regions to move from the store with the biggest region size
	var targetRegion *core.RegionInfo
	var fromStore *core.StoreInfo
	for _, store := range suitableStores {
		// find the region most suitable for moving in the store
		cluster.GetPendingRegionsWithLock(store.GetID(), func(rc core.RegionsContainer) {
			targetRegion = rc.RandomRegion(nil,nil)
		})
		if targetRegion != nil {
			fromStore = store
			break
		}
		cluster.GetFollowersWithLock(store.GetID(), func(rc core.RegionsContainer) {
			targetRegion = rc.RandomRegion(nil,nil)
		})
		if targetRegion != nil {
			fromStore = store
			break
		}
		cluster.GetLeadersWithLock(store.GetID(), func(rc core.RegionsContainer) {
			targetRegion = rc.RandomRegion(nil,nil)
		})
		if targetRegion != nil {
			fromStore = store
			break
		}
	}
	if targetRegion == nil || len(targetRegion.GetPeers()) < cluster.GetMaxReplicas() {
		return nil
	}

	// select the store with the smallest region size as target
	sort.Slice(suitableStores, func(i,j int) bool{
		return suitableStores[i].GetRegionSize() < suitableStores[j].GetRegionSize()
	})
	for _, toStore := range suitableStores {
		if toStore.IsOffline() {
			continue
		}
		// can not move to the store which already has this region
		if targetRegion.GetStorePeer(toStore.GetID()) != nil {
			continue
		}
		// judge whether this movement is valuable
		if (fromStore.GetRegionSize() - toStore.GetRegionSize()) > 2 * targetRegion.GetApproximateSize() {
			// move
			newPeer, err := cluster.AllocPeer(toStore.GetID())
			if err != nil {
				return nil
			}
			op, err := operator.CreateMovePeerOperator("balance_region", cluster, targetRegion, operator.OpBalance, fromStore.GetID(), toStore.GetID(), newPeer.GetId())
			if err != nil {
				return nil
			}
			return op
		}
	}

	return nil
}
