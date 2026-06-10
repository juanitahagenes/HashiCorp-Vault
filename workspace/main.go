package main

import (
	"context"
	"fmt"
	"time"

	"github.com/juanitahagenes/HashiCorp-Vault/vault"
)

func main() {
	fmt.Println("Starting Vault HA Lease Renewal Simulation...")
	core := vault.NewCore()
	router := vault.NewRouter(core)

	core.SetActive()
	core.ApplyRaftLogs(10)

	// Try to renew immediately
	resp, err := router.HandleRenewLease(context.Background(), "test-lease", 1*time.Hour)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Immediate renewal status: %d (Expected 503)\n", resp.StatusCode)

	// Wait for active procedures to complete
	err = core.WaitForActiveProcedures(context.Background())
	if err != nil {
		fmt.Printf("Error waiting: %v\n", err)
		return
	}

	// Try to renew again
	resp, err = router.HandleRenewLease(context.Background(), "test-lease", 1*time.Hour)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Post-activation renewal status: %d (Expected 200)\n", resp.StatusCode)
}
