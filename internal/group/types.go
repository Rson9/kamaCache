package group

import (
	"errors"
	"sync"
)

var (
	groupsMu sync.RWMutex
	groups   = make(map[string]*Group)
)

var (
	ErrKeyRequired = errors.New("key is required")
	ErrGroupClosed = errors.New("cache group is closed")
)
