package grasure

import "github.com/DurantVivado/reedsolomon"

// Scale expands the storage system to a new k and new m, for example,
// Start with a (2,1) system but with more data flouring into, the system needs to be scaled to
// a larger system, say (6,4).
//
//One advantage is that a bigger k supports higher storage efficiency.
//
//Another is that requirement of fault tolerance may level up when needed.
//
//It unavoidably incurrs serious data migration. We are working to minimize the traffic.
func (e *Erasure) Scale(new_k, new_m int) error {
	if new_k <= 0 || new_m <= 0 {
		return reedsolomon.ErrInvShardNum
	}
	if new_k+new_m > 256 {
		return reedsolomon.ErrMaxShardNum
	}
	if new_k+new_m > e.DiskNum {
		return errTooFewDisksAlive
	}
	if new_k == e.K && new_m == e.M {
		return nil
	}
	//step 1: modify the struct
	//step 2: migrate data as well as replicas
	//step 3: reorganize layout to conform to randomized distribution (other forms applies)
	//step 4: write the new config and update replicas
	return nil
}
