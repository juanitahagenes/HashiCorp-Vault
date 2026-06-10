package vault

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestLeaseRenewalDuringPromotion(t *testing.T) {
	core := NewCore()
	router := NewRouter(core)

	// Initially standby
	resp, err := router.HandleRenewLease(context.Background(), "test-lease", 1*time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Errorf("expected 307, got %d", resp.StatusCode)
	}

	// Promote to active
	core.SetActive()

	// Immediately bombard with renewal requests
	var wg sync.WaitGroup
	results := make(chan int, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Try to renew
			resp, err := router.HandleRenewLease(context.Background(), "test-lease", 1*time.Hour)
			if err != nil {
				return
			}
			results <- resp.StatusCode
		}()
	}

	wg.Wait()
	close(results)

	var successCount, retryCount, otherCount int
	for code := range results {
		switch code {
		case http.StatusOK:
			successCount++
		case http.StatusServiceUnavailable:
			retryCount++
		default:
			otherCount++
		}
	}

	t.Logf("Success: %d, Retry (503): %d, Other: %d", successCount, retryCount, otherCount)

	if otherCount > 0 {
		t.Errorf("Expected no other status codes, but got some (e.g. 404/400)")
	}

	// Wait for active procedures to complete
	err = core.WaitForActiveProcedures(context.Background())
	if err != nil {
		t.Fatalf("failed to wait for active procedures: %v", err)
	}

	// Now renewal must succeed
	resp, err = router.HandleRenewLease(context.Background(), "test-lease", 1*time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestStepDownCleansUp(t *testing.T) {
	core := NewCore()
	core.SetActive()

	err := core.WaitForActiveProcedures(context.Background())
	if err != nil {
		t.Fatalf("failed to wait for active procedures: %v", err)
	}

	core.StepDown()

	if !core.IsStandby() {
		t.Errorf("expected core to be standby")
	}

	if core.expirationManager.IsReady() {
		t.Errorf("expected expiration manager to not be ready after step down")
	}
}

func TestLeaseRenewalRaftLag(t *testing.T) {
	core := NewCore()
	router := NewRouter(core)

	// Promote to active
	core.SetActive()

	// Try to renew, should block and return 503 because Raft lag is not caught up
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	resp, err := router.HandleRenewLease(ctx, "test-lease", 1*time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", resp.StatusCode)
	}

	// Catch up Raft logs
	core.ApplyRaftLogs(10)

	// Wait for active procedures to complete
	err = core.WaitForActiveProcedures(context.Background())
	if err != nil {
		t.Fatalf("failed to wait for active procedures: %v", err)
	}

	// Now renewal must succeed
	resp, err = router.HandleRenewLease(context.Background(), "test-lease", 1*time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestSlowRestoreBlocksRenewals(t *testing.T) {
	core := NewCore()
	// Set Raft already caught up so we only test ExpirationManager restore delay
	core.ApplyRaftLogs(10)
	router := NewRouter(core)

	core.SetActive()

	// Immediately try to renew, should block and return 503 because ExpirationManager is slow
	resp, err := router.HandleRenewLease(context.Background(), "test-lease", 1*time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", resp.StatusCode)
	}

	// Wait for restore to complete (takes 100ms in our mock)
	time.Sleep(150 * time.Millisecond)

	// Now renewal must succeed
	resp, err = router.HandleRenewLease(context.Background(), "test-lease", 1*time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}
