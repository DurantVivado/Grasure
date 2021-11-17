package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"
)

//consult user to avoid maloperation
func consultUserBeforeAction() (bool, error) {
	log.Println("If you are sure to proceed, type:\n [Y]es or [N]o.")

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
func genRandomArr(n int) []int {
	shuff := make([]int, n)
	for i := 0; i < n; i++ {
		shuff[i] = i
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
func makeArr2D(row, col int) [][]byte {
	out := make([][]byte, row)
	for i := range out {
		out[i] = make([]byte, col)
	}
	return out
}
