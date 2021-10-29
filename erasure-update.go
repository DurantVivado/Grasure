package main

//update a file according to a new file, the local `filename` will be used to update the file in the cloud with the same name
func (e *Erasure) update(oldFile, newFile string) error {
	//two ways are available, one is RWM, other one is RCW
	//we minimize the parity computation and transferring overhead as much as possible
	// fi, ok := e.fileMap[oldFile]
	// if !ok {
	// 	return ErrFileNotFound
	// }
	// //read new file
	// f, err := os.Open(newFile)
	// if err != nil {
	// 	return err
	// }
	// defer f.Close()
	// fileInfo, err := f.Stat()
	// if err != nil {
	// 	return err
	// }
	// //we allocate the buffer actually the same size of the file
	// // fmt.Println(fileInfo)
	// oldFileSize := fi.fileSize
	// fi.fileSize = fileInfo.Size()
	// buf := bufio.NewReader(f)
	// data := make([]byte, fi.fileSize)
	// _, err = buf.Read(data)
	// if err != nil {
	// 	return err
	// }
	// //if the len of local file is 0, we regard as deleting
	// if len(data) == 0 {
	// 	log.Println("The input file length is 0, this will lead to removal of present file")
	// 	// if ans, err := consultUserBeforeAction(); !ans {
	// 	// 	return nil
	// 	// } else if err != nil {
	// 	// 	return err
	// 	// }
	// 	e.removeFile(oldFile)
	// 	return e.writeConfig()
	// }
	// h := sha256.New()
	// f.Seek(0, 0)
	// if _, err := io.Copy(h, f); err != nil {
	// 	return err
	// }
	// fi.hash = fmt.Sprintf("%x", h.Sum(nil))
	// //we split the data, and create empty parity
	// newShards, err := e.enc.Split(data)
	// if err != nil {
	// 	return err
	// }
	// //we read old shards
	// oldShards := make([][]byte, e.k+e.m)
	// g := new(errgroup.Group)
	// for i, path := range e.diskInfos {
	// 	i := i
	// 	path := path
	// 	g.Go(func() error {
	// 		var partPath string
	// 		if fi.distribution[i] < e.k { //data block
	// 			partPath = path.diskPath + "/" + fi.fileName + "/D_" + fmt.Sprintf("%d", fi.distribution[i])
	// 		} else {
	// 			partPath = path.diskPath + "/" + fi.fileName + "/P_" + fmt.Sprintf("%d", fi.distribution[i])

	// 		}
	// 		f, err := os.Open(partPath)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		defer f.Close()
	// 		blockByte := (int(oldFileSize) + e.k - 1) / e.k
	// 		oldShards[fi.distribution[i]] = make([]byte, blockByte)
	// 		f.Read(oldShards[fi.distribution[i]])
	// 		if err != nil {
	// 			return err
	// 		}
	// 		return nil
	// 	})
	// }
	// if err := g.Wait(); err != nil {
	// 	return err
	// }
	// err = e.enc.Update(oldShards, newShards[:e.k])
	// if err != nil {
	// 	return err
	// }
	// //if a block is updated, we replace it, otherwise remain intact
	// //and all parity blocks are bound to update
	// for i, path := range e.diskInfos {
	// 	i := i
	// 	path := path
	// 	g.Go(func() error {
	// 		var partPath string
	// 		if fi.distribution[i] < e.k { //data block
	// 			partPath = path.diskPath + "/" + fi.fileName + "/D_" + fmt.Sprintf("%d", fi.distribution[i])
	// 		} else {
	// 			partPath = path.diskPath + "/" + fi.fileName + "/P_" + fmt.Sprintf("%d", fi.distribution[i])

	// 		}
	// 		nf, err := os.OpenFile(partPath, os.O_WRONLY|os.O_TRUNC, 0666)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		defer nf.Close()
	// 		buf := bufio.NewWriter(nf)
	// 		_, err = buf.Write(oldShards[fi.distribution[i]])
	// 		if err != nil {
	// 			return err
	// 		}
	// 		buf.Flush()
	// 		return nil
	// 	})
	// }
	// if err := g.Wait(); err != nil {
	// 	return err
	// }

	// //write into config
	// err = e.writeConfig()
	// if err != nil {
	// 	return err
	// }
	// log.Println("Successfully updated.")
	return nil
}
