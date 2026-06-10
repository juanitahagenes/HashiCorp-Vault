package vault

import (
	"context"
	"errors"
	"sync"
	"time"
)

type ExpirationManager struct {
	core       *Core
	mu         sync.RWMutex
	leases     map[string]time.Time
	restored   bool
	restoreCh  chan struct{}
	stopCh     chan struct{}
}

func NewExpirationManager(c *Core) *ExpirationManager {
	return &ExpirationManager{
		core:      c,
		leases:    make(map[string]time.Time),
		restoreCh: make(chan struct{}),
		stopCh:    make(chan struct{}),
	}
}

func (em *ExpirationManager) Restore(ctx context.Context) error {
	em.mu.Lock()
	em.restored = false
	em.restoreCh = make(chan struct{})
	em.stopCh = make(chan struct{})
	em.mu.Unlock()

	// Simulate loading leases from physical storage (e.g., Raft/Consul)
	// We can simulate a delay
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	case <-em.stopCh:
		return errors.New("stopped")
	}

	em.mu.Lock()
	em.leases["test-lease"] = time.Now().Add(1 * time.Hour)
	em.restored = true
	close(em.restoreCh)
	em.mu.Unlock()

	return nil
}

func (em *ExpirationManager) Stop() {
	em.mu.Lock()
	defer em.mu.Unlock()
	select {
	case <-em.stopCh:
		// already stopped
	default:
		close(em.stopCh)
	}
	em.restored = false
}

func (em *ExpirationManager) IsReady() bool {
	em.mu.RLock()
	defer em.mu.RUnlock()
	return em.restored
}

func (em *ExpirationManager) Renew(leaseID string, increment time.Duration) (time.Time, error) {
	em.mu.Lock()
	defer em.mu.Unlock()

	if !em.restored {
		return time.Time{}, errors.New("expiration manager not ready")
	}

	expireTime, exists := em.leases[leaseID]
	if !exists {
		return time.Time{}, errors.New("lease not found")
	}

	newExpire := expireTime.Add(increment)
	em.leases[leaseID] = newExpire
	return newExpire, nil
}
