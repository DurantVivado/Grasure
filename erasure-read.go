package main

//read file on the system and return byte stream, include recovering
func (e *Erasure) read(filename string, savepath string) error {
	// fi, ok := e.fileMap[filename]
	// if !ok {
	// 	return ErrFileNotFound
	// }
	// g := new(errgroup.Group)
	// survivalParity := []int{}
	// survivalData := []int{}
	// var mu sync.Mutex
	//first we check whether the remaining parts are enough for decoding
	// for i, path := range e.diskInfos {
	// 	basePath := path.diskPath
	// 	i := i
	// 	if !path.available {
	// 		continue
	// 	}
	// 	g.Go(func() error {
	// 		var fullPath string
	// 		if fi.distribution[i] < e.k {
	// 			fullPath = basePath + "/" + filename + "/D_" + fmt.Sprintf("%d", fi.distribution[i])
	// 		} else {

	// 			fullPath = basePath + "/" + filename + "/P_" + fmt.Sprintf("%d", fi.distribution[i])
	// 		}
	// 		ex, err := PathExist(fullPath)
	// 		if err != nil {
	// 			return fmt.Errorf("%s fail with error: %s", fullPath, err.Error())
	// 		}
	// 		if ex {
	// 			if fi.distribution[i] < e.k {
	// 				// log.Println(fullPath)
	// 				survivalData = append(survivalData, i)
	// 			} else {
	// 				// log.Println(fullPath)
	// 				survivalParity = append(survivalParity, i)
	// 			}
	// 		}
	// 		return nil
	// 	})

	// }
	// if err := g.Wait(); err != nil {
	// 	return err
	// }

	// if len(survivalData)+len(survivalParity) < e.k {
	// 	return ErrSurvivalNotEnoughForDecoding
	// }
	// //We need to decode the file using parity
	// totalBytes := make([][]byte, e.k+e.m)
	// if len(survivalData)+len(survivalParity) == e.k+e.m {

	// 	//no need for reconstructing
	// 	for _, ind := range survivalData {
	// 		ind := ind
	// 		g.Go(func() error {
	// 			filepath := e.diskInfos[ind].diskPath + "/" + filename + "/D_" + fmt.Sprintf("%d", fi.distribution[ind])
	// 			f, err := os.Open(filepath)
	// 			if err != nil {
	// 				return err
	// 			}
	// 			defer f.Close()
	// 			blockByte := (int(fi.fileSize) + e.k - 1) / e.k
	// 			totalBytes[fi.distribution[ind]] = make([]byte, blockByte)
	// 			f.Read(totalBytes[fi.distribution[ind]])
	// 			f.Sync()
	// 			if err != nil {
	// 				return err
	// 			}
	// 			return nil
	// 		})
	// 	}
	// 	if err := g.Wait(); err != nil {
	// 		return err
	// 	}
	// } else {
	// 	survivalData = append(survivalData, survivalParity...)

	// 	rand.Seed(time.Now().UnixNano())
	// 	rand.Shuffle(len(survivalData), func(i, j int) { survivalData[i], survivalData[j] = survivalData[j], survivalData[i] })
	// 	//for minimizing read overhead, we only choose first k blocks
	// 	for _, ind := range survivalData[:e.k] {
	// 		ind := ind
	// 		g.Go(func() error {
	// 			var fullpath string
	// 			if fi.distribution[ind] < e.k {
	// 				fullpath = e.diskInfos[ind].diskPath + "/" + filename + "/D_" + fmt.Sprintf("%d", fi.distribution[ind])
	// 			} else {
	// 				fullpath = e.diskInfos[ind].diskPath + "/" + filename + "/P_" + fmt.Sprintf("%d", fi.distribution[ind])

	// 			}

	// 			f, err := os.Open(fullpath)
	// 			if err != nil {
	// 				return err
	// 			}
	// 			defer f.Close()
	// 			blockByte := (int(fi.fileSize) + e.k - 1) / e.k
	// 			totalBytes[fi.distribution[ind]] = make([]byte, blockByte)
	// 			_, err = f.Read(totalBytes[fi.distribution[ind]])
	// 			if err != nil {
	// 				return err
	// 			}

	// 			return nil
	// 		})
	// 	}
	// 	if err := g.Wait(); err != nil {
	// 		return err
	// 	}
	// 	//reconstructing, we first decode data, once completed
	// 	//notify the customer, and parity reconstruction, we move it to back-end

	// 	err = e.enc.Reconstruct(totalBytes)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	// f, err := os.OpenFile(savepath, os.O_CREATE|os.O_RDWR, 0666)
	// if err != nil {
	// 	return err
	// }
	// defer f.Close()
	// err = e.enc.Join(f, totalBytes, int(fi.fileSize))
	// if err != nil {
	// 	return err
	// }
	// //checksum
	// f.Seek(0, 0)
	// h := sha256.New()
	// if _, err := io.Copy(h, f); err != nil {
	// 	return nil
	// }
	// hashSum := fmt.Sprintf("%x", h.Sum(nil))
	// if strings.Compare(hashSum, fi.hash) != 0 {
	// 	return ErrFileIncompleted
	// }
	// log.Printf("%s successfully read (Decoded)!", filename)
	return nil
}
