package grasure

import "math"

//Examplar random distribution layout generator
//Two structure need specialized: fi.blockToOffset and fi.Distribution
func (e *Erasure) generateLayout(fi *fileInfo) {
	if fi == nil {
		return
	}
	stripeNum := int(ceilFracInt64(fi.FileSize, e.dataStripeSize))
	fi.Distribution = make([][]int, stripeNum)
	fi.blockToOffset = makeArr2DInt(stripeNum, e.K+e.M)
	countSum := make([]int, e.DiskNum)
	for i := 0; i < stripeNum; i++ {
		fi.Distribution[i] = genRandomArr(e.DiskNum, 0)[:e.K+e.M]
		// fi.Distribution[i] = getSeqArr(e.K + e.M)
		for j := 0; j < e.K+e.M; j++ {
			diskId := fi.Distribution[i][j]
			fi.blockToOffset[i][j] = countSum[diskId]
			countSum[diskId]++
		}
	}
}

func (e *Erasure) generateStripeInfo(fi *fileInfo) {
	if fi == nil {
		return
	}
	stripeNum := int(ceilFracInt64(fi.FileSize, e.dataStripeSize))
	distNum := ceilFracInt(e.DiskNum, intBit)
	for i := 0; i < stripeNum; i++ {
		dist := fi.Distribution[i]
		blockToOffset := fi.blockToOffset[i]
		distBit := make([]uint64, distNum)
		for j := 0; j < e.K+e.M; j++ {
			var mask uint64 = 1
			mask <<= uint64(dist[j] % intBit)
			distBit[dist[j]/intBit] |= mask
			// fmt.Printf("%b %d\n", distBit[dist[j]/intBit], distBit[dist[j]/intBit])
		}
		spInfo := &stripeInfo{StripeId: e.StripeNum + int64(i), DistNum: distNum, DistBit: distBit, Dist: dist, BlockToOffset: make([]int, len(blockToOffset))}
		spInfo.BlockToOffset = blockToOffset
		e.Stripes = append(e.Stripes, spInfo)
	}
	// for _, s := range e.Stripes {
	// 	fmt.Printf("%b\n", s.DistBit.dist)
	// }
}

func (e *Erasure) bitToDist(distBit []uint64, distNum int) []int {
	dist := make([]int, 0)
	for i := 0; i < distNum; i++ {
		var mask uint64 = 1
		cnt := 0
		for cnt < 64 {
			if distBit[i]&mask != 0 {
				dist = append(dist, i*intBit+int(math.Log2(float64(mask))))
			}
			mask <<= 1
			cnt++
		}
	}
	return dist
}
