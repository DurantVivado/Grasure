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
	e.generateStripeInfo(fi)
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
		spDist := &stripeDistBit{num: distNum, dist: make([]uint64, distNum)}
		for j := 0; j < e.K+e.M; j++ {
			var mask uint64 = 1
			mask <<= uint64(dist[j] % intBit)
			spDist.dist[dist[j]/intBit] |= mask
		}
		spInfo := &stripeInfo{stripeId: e.stripeNum + int64(i), DistBit: spDist, blockToOffset: blockToOffset}
		e.stripes = append(e.stripes, spInfo)
	}
}

func (e *Erasure) bitToDist(bit *stripeDistBit) []int {
	dist := make([]int, e.K+e.M)
	for i := 0; i < bit.num; i++ {
		var mask uint64 = 1
		for mask/2 < 63 {
			if bit.dist[i]&mask == 1 {
				dist = append(dist, i*intBit+int(mask/2))
			}
			mask <<= 1
		}
	}
	return dist
}
