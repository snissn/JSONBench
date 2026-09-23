package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEnginePrepareReservationCommitCannotStarve(t *testing.T) {
	quota := newEnginePrepareReservation(16)
	if err := quota.acquire(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	first, hadOther, err := quota.acquireAvailable(context.Background(), 2, 2)
	if err != nil || first != 12 || hadOther {
		t.Fatalf("prepare credit=%d hadOther=%v err=%v", first, hadOther, err)
	}
	quota.shrink(14, 12)
	acquired := make(chan int64, 1)
	go func() {
		if err := quota.acquire(context.Background(), 2); err != nil {
			return
		}
		credit, _, err := quota.acquireAvailable(context.Background(), 2, 2)
		if err == nil {
			acquired <- credit + 2
		}
	}()
	blocked, cancel := context.WithCancel(context.Background())
	cancel()
	if err := quota.acquire(blocked, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled acquire with four credits available: %v", err)
	}
	quota.release(12)
	select {
	case credit := <-acquired:
		if credit < 4 || credit > 16 {
			t.Fatalf("successor reservation=%d want 4..16", credit)
		}
		quota.release(credit)
	case <-time.After(time.Second):
		t.Fatal("successor remained blocked after commit released")
	}
	if quota.peak() > 16 {
		t.Fatalf("peak reservation=%d exceeds 16", quota.peak())
	}
}

func TestEnginePrepareReservationRejectsInvalidAccounting(t *testing.T) {
	quota := newEnginePrepareReservation(16)
	if err := quota.acquire(context.Background(), 16); err != nil {
		t.Fatal(err)
	}
	credit := int64(16)
	for _, test := range []struct {
		name string
		fn   func()
	}{
		{"grow on shrink", func() { quota.shrink(credit, credit+1) }},
		{"release too much", func() { quota.release(credit + 1) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("invalid accounting did not panic")
				}
			}()
			test.fn()
		})
	}
	quota.release(credit)
}

func TestEnginePrepareReservationSourceOverlapsCommit(t *testing.T) {
	quota := newEnginePrepareReservation(16)
	if err := quota.acquire(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	extra, hadOther, err := quota.acquireAvailable(context.Background(), 2, 2)
	if err != nil || extra != 12 || hadOther {
		t.Fatalf("prepare credit=%d hadOther=%v err=%v", extra, hadOther, err)
	}
	quota.shrink(14, 6) // admitted commit token
	if err := quota.acquire(context.Background(), 2); err != nil {
		t.Fatalf("source could not advance during commit: %v", err)
	}
	blocked, cancel := context.WithCancel(context.Background())
	cancel()
	if err := quota.waitForOtherOwner(blocked, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("depth-zero prepare did not wait for commit: %v", err)
	}
	quota.release(6)
	if err := quota.waitForOtherOwner(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	quota.release(2)
}

func TestEnginePrepareReservationRemembersOtherOwnerAtAdmission(t *testing.T) {
	quota := newEnginePrepareReservation(16)
	if err := quota.acquire(context.Background(), 6); err != nil {
		t.Fatal(err)
	}
	if err := quota.acquire(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	extra, hadOther, err := quota.acquireAvailable(context.Background(), 2, 2)
	if err != nil || extra != 6 || !hadOther {
		t.Fatalf("prepare credit=%d hadOther=%v err=%v", extra, hadOther, err)
	}
	quota.release(6) // Predecessor can complete before preparation rejects the old credit.
	if !hadOther {
		t.Fatal("lost predecessor ownership after release")
	}
	quota.release(8)
}
