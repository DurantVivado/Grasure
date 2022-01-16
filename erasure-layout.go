package grasure

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
			mask <<= uint64(dist[j]%intBit) - 1
			distBit[dist[j]/intBit] |= mask
			// fmt.Printf("%b\n", spDist.dist[dist[j]/intBit])
		}
		spInfo := &stripeInfo{StripeId: e.StripeNum + int64(i), DistNum: distNum, DistBit: distBit, BlockToOffset: make([]int, len(blockToOffset))}
		spInfo.BlockToOffset = blockToOffset
		e.Stripes = append(e.Stripes, spInfo)
	}
	// for _, s := range e.Stripes {
	// 	fmt.Printf("%b\n", s.DistBit.dist)
	// }
}

func (e *Erasure) bitToDist(distBit []uint64, distNum int) []int {
	dist := make([]int, e.K+e.M)
	for i := 0; i < distNum; i++ {
		var mask uint64 = 1
		for mask/2 < 63 {
			if distBit[i]&mask == 1 {
				dist = append(dist, i*intBit+int(mask/2))
			}
			mask <<= 1
		}
	}
	return dist
}
