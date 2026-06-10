package vault

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Core struct {
	stateLock sync.RWMutex
	isStandby uint32 // 1 for standby, 0 for active
	
	expirationManager *ExpirationManager
	
	// Raft simulation
	raftCommitIndex  uint64
	raftAppliedIndex uint64
	
	// readiness flag/channel
	postActiveProceduresComplete chan struct{}
	activeProceduresStarted      uint32
}

func NewCore() *Core {
	c := &Core{
		isStandby:                    1,
		postActiveProceduresComplete: make(chan struct{}),
		raftCommitIndex:              100,
		raftAppliedIndex:             90, // starts with some lag
	}
	c.expirationManager = NewExpirationManager(c)
	return c
}

func (c *Core) IsStandby() bool {
	return atomic.LoadUint32(&c.isStandby) == 1
}

func (c *Core) SetActive() {
	atomic.StoreUint32(&c.isStandby, 0)
	// Start active procedures
	if atomic.CompareAndSwapUint32(&c.activeProceduresStarted, 0, 1) {
		go c.runActiveProcedures()
	}
}

func (c *Core) StepDown() {
	atomic.StoreUint32(&c.isStandby, 1)
	// Reset procedures complete channel
	c.stateLock.Lock()
	c.postActiveProceduresComplete = make(chan struct{})
	atomic.StoreUint32(&c.activeProceduresStarted, 0)
	c.stateLock.Unlock()
	
	c.expirationManager.Stop()
}

func (c *Core) runActiveProcedures() {
	// Simulate waiting for Raft replication lag (applying committed logs)
	for {
		c.stateLock.Lock()
		applied := c.raftAppliedIndex
		committed := c.raftCommitIndex
		c.stateLock.Unlock()
		
		if applied >= committed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Simulate restoring expiration manager
	ctx := context.Background()
	c.expirationManager.Restore(ctx)
	
	// Signal that active procedures are complete
	c.stateLock.Lock()
	close(c.postActiveProceduresComplete)
	c.stateLock.Unlock()
}

func (c *Core) WaitForActiveProcedures(ctx context.Context) error {
	c.stateLock.RLock()
	ch := c.postActiveProceduresComplete
	c.stateLock.RUnlock()
	
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Core) ApplyRaftLogs(count uint64) {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	c.raftAppliedIndex += count
}
