package vault

import (
	"context"
	"errors"
	"net/http"
	"time"
)

type Response struct {
	StatusCode int
	Body       string
	Header     http.Header
}

type Router struct {
	core *Core
}

func NewRouter(c *Core) *Router {
	return &Router{core: c}
}

func (r *Router) HandleRenewLease(ctx context.Context, leaseID string, increment time.Duration) (*Response, error) {
	if r.core.IsStandby() {
		return &Response{
			StatusCode: http.StatusTemporaryRedirect,
			Body:       "node is standby",
		}, nil
	}

	// Check if active procedures (including expiration manager restore) are complete.
	// We can block briefly with a timeout, or return 503 Service Unavailable with Retry-After.
	// Let's block briefly (e.g., up to 50ms) and if still not ready, return 503.
	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	err := r.core.WaitForActiveProcedures(waitCtx)
	if err != nil {
		// If it timed out or context was canceled, return 503 with Retry-After
		if errors.Is(err, context.DeadlineExceeded) {
			header := make(http.Header)
			header.Set("Retry-After", "1")
			return &Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       "Expiration manager is initializing, please retry",
				Header:     header,
			}, nil
		}
		return nil, err
	}

	// Now call Renew
	newExpire, err := r.core.expirationManager.Renew(leaseID, increment)
	if err != nil {
		if err.Error() == "lease not found" {
			return &Response{
				StatusCode: http.StatusNotFound,
				Body:       err.Error(),
			}, nil
		}
		return &Response{
			StatusCode: http.StatusBadRequest,
			Body:       err.Error(),
		}, nil
	}

	return &Response{
		StatusCode: http.StatusOK,
		Body:       newExpire.Format(time.RFC3339),
	}, nil
}
