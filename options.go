package main

import (
	"github.com/klauspost/cpuid/v2"
	"github.com/klauspost/reedsolomon"
)

// Option allows to override processing parameters.
type Option func(*options)

type options struct {
	rO   reedsolomon.Option
	mode string
}
