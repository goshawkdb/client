package client

import (
	"github.com/go-kit/kit/log"
)

type DebugLogFunc func(log.Logger, ...interface{})

var DebugLog = DebugLogFunc(func(log.Logger, ...interface{}) {})
