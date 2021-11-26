package main

//Examplar random distribution layout generator
//Two structure need specialized: fi.blockToOffset and fi.Distribution
func (e *Erasure) generateLayout(fi *FileInfo) {
	if fi == nil {
		return
	}
	stripeNum := int(ceilFracInt64(fi.FileSize, e.dataStripeSize))
	diskNum := len(e.diskInfos)
	fi.Distribution = make([][]int, stripeNum)
	fi.blockToOffset = makeArr2DInt(stripeNum, e.K+e.M)
	countSum := make([]int, len(e.diskInfos))
	for i := 0; i < stripeNum; i++ {
		fi.Distribution[i] = genRandomArr(diskNum, 0)[:e.K+e.M]
		for j := 0; j < e.K+e.M; j++ {
			diskId := fi.Distribution[i][j]
			fi.blockToOffset[i][j] = countSum[diskId]
			countSum[diskId]++
		}

	}
}
