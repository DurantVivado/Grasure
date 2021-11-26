package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

//consult user to avoid maloperation
func consultUserBeforeAction() (bool, error) {
	fmt.Println("If you are sure to proceed, type:\n [Y]es or [N]o.")

	inputReader := bufio.NewReader(os.Stdin)
	for {
		ans, err := inputReader.ReadString('\n')
		if err != nil {
			return false, err
		}
		ans = strings.TrimSuffix(ans, "\n")
		if ans == "Y" || ans == "y" || ans == "Yes" || ans == "yes" {
			return true, nil
		} else if ans == "N" || ans == "n" || ans == "No" || ans == "no" {
			return false, nil
		} else {
			fmt.Println("Please do not make joke")
		}
	}

}

//an instant error dealer
var failOnErr = func(mode string, e error) {
	if e != nil {
		log.Fatalf("%s: %s", mode, e.Error())
	}
}

//look if path exists
func PathExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

//ceilFrac return (a+b-1)/b
func ceilFracInt(a, b int) int {
	return (a + b - 1) / b
}

//ceilFrac return (a+b-1)/b
func ceilFracInt64(a, b int64) int64 {
	return (a + b - 1) / b
}

func min(args ...int) int {
	if len(args) == 0 {
		return 0x7fffffff
	}
	ret := args[0]
	for _, arg := range args {
		if arg < ret {
			ret = arg
		}
	}
	return ret
}

func max(args ...int) int {
	if len(args) == 0 {
		return 0xffffffff
	}
	ret := args[0]
	for _, arg := range args {
		if arg > ret {
			ret = arg
		}
	}
	return ret
}

//each stripe randomized distribution
func genRandomArr(n, start int) []int {
	shuff := make([]int, n)
	for i := 0; i < n; i++ {
		shuff[i] = i + start
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(shuff), func(i, j int) { shuff[i], shuff[j] = shuff[j], shuff[i] })
	return shuff
}

//get arr of default sequence
func getSeqArr(n int) []int {
	out := make([]int, n)
	for i := 0; i < n; i++ {
		out[i] = i
	}
	return out
}

//classical robin-round style
//e.g.
//1 2 3 4 5
//5 1 2 3 4
//4 5 1 2 3
//...
func rightRotateLayout(row, col int) [][]int {
	arr2D := make([][]int, row)
	for i := 0; i < row; i++ {
		arr2D[i] = make([]int, col)
		for j := 0; j < col; j++ {
			arr2D[i][j] = (j - i + col) % col
		}
	}
	return arr2D
}

func monitorCancel(cancel context.CancelFunc) {
	channel := make(chan os.Signal, 2)
	signal.Notify(channel, syscall.SIGINT, syscall.SIGTERM)
	<-channel
	cancel()
}

func goroutineNum() int {
	return runtime.NumGoroutine()
}

//make an 2D byte slice
func makeArr2DByte(row, col int) [][]byte {
	out := make([][]byte, row)
	for i := range out {
		out[i] = make([]byte, col)
	}
	return out
}

//make an 2D int slice
func makeArr2DInt(row, col int) [][]int {
	out := make([][]int, row)
	for i := range out {
		out[i] = make([]int, col)
	}
	return out
}

//check if two file are completely same
//warning: use io.copy
func checkFileIfSame(dst, src string) (bool, error) {
	if ok, err := PathExist(dst); err != nil || !ok {
		return false, err
	}
	if ok, err := PathExist(src); err != nil || !ok {
		return false, err
	}
	fdst, err := os.Open(dst)
	if err != nil {
		return false, err
	}
	defer fdst.Close()
	fsrc, err := os.Open(src)
	if err != nil {
		return false, err
	}
	defer fsrc.Close()
	hashDst, err := hashStr(fdst)
	if err != nil {
		return false, err
	}
	hashSrc, err := hashStr(fsrc)
	if err != nil {
		return false, err
	}
	return hashDst == hashSrc, nil
}

//retain hashstr
func hashStr(f *os.File) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	out := fmt.Sprintf("%x", h.Sum(nil))
	return out, nil
}

//fillRandom
func fillRandom(p []byte) {
	for i := 0; i < len(p); i += 7 {
		val := rand.Int63()
		for j := 0; i+j < len(p) && j < 7; j++ {
			p[i+j] = byte(val)
			val >>= 8
		}
	}
}

//string2Slice
func stringToSlice2D(s string) [][]int {
	s = strings.Trim(s, "[]\n")
	strs := strings.Split(s, ",")
	row := len(strs)
	out := make([][]int, row)
	for i := 0; i < row; i++ {
		sub := strings.Trim(strs[i], "[]\n")
		for _, num := range strings.Split(sub, ",") {
			n, _ := strconv.Atoi(num)
			out[i] = append(out[i], n)
		}
	}
	return out
}
