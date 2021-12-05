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
