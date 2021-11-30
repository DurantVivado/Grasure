package main

import (
	"math/rand"
	"time"
)

//simulate disk failure or bitrot
func (e *Erasure) destroy(mode string, failNum int) {
	if mode == "diskFail" {
		if failNum == 0 {
			return
		}
		//we randomly picked up failNum disks and mark as unavailable
		shuff := make([]int, len(e.diskInfos))
		for i := 0; i < len(e.diskInfos); i++ {
			shuff[i] = i
		}
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(shuff), func(i, j int) { shuff[i], shuff[j] = shuff[j], shuff[i] })
		// log.Println("simulate on failure of:")
		for i := 0; i < failNum; i++ {
			// fmt.Println(e.diskInfos[shuff[i]].diskPath)
			e.diskInfos[shuff[i]].available = false
		}
	} else if mode == "bitRot" {

	}
}
