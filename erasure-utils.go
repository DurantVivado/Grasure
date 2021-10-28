package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

//consult user to avoid maloperation
func consultUserBeforeAction() (bool, error) {
	log.Println("If you are sure to proceed, type: [Y]es otherwise [N]o.")

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

//ceilFrac return a/b
func ceilFrac(a, b int64) int64 {
	if b == 0 {
		return 0
	}
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

func genRandomArr(n int) []int {
	shuff := make([]int, n)
	for i := 0; i < n; i++ {
		shuff[i] = i
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(shuff), func(i, j int) { shuff[i], shuff[j] = shuff[j], shuff[i] })
	return shuff
}

func monitorCancel(cancel context.CancelFunc) {
	fmt.Println("Press ctrl + s to halt the system")
	channel := make(chan os.Signal)
	signal.Notify(channel, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-channel
	cancel()
}