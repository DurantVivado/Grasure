package grasure

import "log"

func (e *Erasure) ReconstructWithGBlocks(g int) (map[string]string, error) {
	totalFiles := e.getFileNum()
	if !e.Quiet {
		log.Printf("Start recovering, totally %d files need recovery",
			totalFiles)
	} //first, make clear how many disks need to be recovered
	//Second, match backup partners
	//Third, concurrently recover the part of the files
	failNum := 0
	for i := 0; i < e.DiskNum; i++ {
		if !e.diskInfos[i].available {
			failNum++
		}
	}
	if failNum == 0 {
		return nil, nil
	}
	//the failure number exceeds the fault tolerance
	if failNum > 2 {
		return nil, errTooFewDisksAlive
	}
	//the failure number doesn't exceed the fault tolerance
	//but unluckily we don't have enough backups!
	if len(e.diskInfos)-e.DiskNum < 1 {
		return nil, errNotEnoughBackupForRecovery
	}

	//the failed disks are mapped to backup disks
	replaceMap := make(map[int]int)
	ReplaceMap := make(map[string]string)
	// diskFailId := 0
	j := e.DiskNum

	for i := 0; i < e.DiskNum; i++ {
		if !e.diskInfos[i].available {
			ReplaceMap[e.diskInfos[i].diskPath] = e.diskInfos[j].diskPath
			replaceMap[i] = j
			// diskFailId = i
			break
		}
	}

}
