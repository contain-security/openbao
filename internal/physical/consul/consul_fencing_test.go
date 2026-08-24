package consul

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/sdk/v2/physical"
)

// newFencingBackend builds an HA-enabled backend over a prefix unique to this
// run, so concurrent tests cannot contend for the same lock key.
func newFencingBackend(t *testing.T, path string) *ConsulBackend {
	t.Helper()
	cfg := requireConsul(t, path)
	cfg["ha_enabled"] = "true"
	b, err := NewConsulBackend(cfg, hclog.NewNullLogger())
	if err != nil {
		failOrSkip(t, "Consul not available: %v", err)
	}
	return b.(*ConsulBackend)
}

// acquireActiveLock takes the lock and registers it the way core does on
// winning leadership, returning a release function.
func acquireActiveLock(t *testing.T, b *ConsulBackend, key, value string) (*ConsulLock, func()) {
	t.Helper()

	l, err := b.LockWith(key, value)
	if err != nil {
		t.Fatalf("LockWith failed: %v", err)
	}
	lock := l.(*ConsulLock)

	stopCh := make(chan struct{})
	leaderCh, err := lock.Lock(stopCh)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}
	if leaderCh == nil {
		t.Fatal("expected a leader channel after acquiring the lock")
	}

	if err := b.RegisterActiveNodeLock(lock); err != nil {
		t.Fatalf("RegisterActiveNodeLock failed: %v", err)
	}

	return lock, func() {
		_ = lock.Unlock()
		close(stopCh)
	}
}

// TestConsulBackend_Fencing_WritesRejectedAfterLosingLock is the property the
// whole mechanism exists for: once this node's session is gone, its writes
// must not land, even though it has not yet noticed it lost leadership.
//
// Without fencing the write succeeds, and a former leader can corrupt the
// barrier while a successor is already writing to it.
func TestConsulBackend_Fencing_WritesRejectedAfterLosingLock(t *testing.T) {
	backend := newFencingBackend(t, fmt.Sprintf("test/openbao/fencing-lost-%d/", time.Now().UnixNano()))
	ctx := context.Background()

	lock, release := acquireActiveLock(t, backend, "core/lock", "node-a")
	defer release()

	// While the lock is held, writes go through.
	if err := backend.Put(ctx, &physical.Entry{Key: "fenced/ok", Value: []byte("before")}); err != nil {
		t.Fatalf("write while holding the lock should succeed: %v", err)
	}

	// Kill the session out from under the node. This is what a partition or
	// a paused process looks like from Consul's side: the session lapses
	// while the process still believes it is active.
	_, session := lock.Info()
	if session == "" {
		t.Fatal("lock reported no session to fence against")
	}
	if _, err := backend.client.Session().Destroy(session, nil); err != nil {
		t.Fatalf("failed to invalidate the session: %v", err)
	}

	err := backend.Put(ctx, &physical.Entry{Key: "fenced/blocked", Value: []byte("after")})
	if err == nil {
		t.Fatal("write succeeded after the lock session was invalidated: a demoted leader can still corrupt storage")
	}
	if !errors.Is(err, ErrLostActiveLock) {
		t.Fatalf("expected the write to report a lost lock, got: %v", err)
	}

	// The value must genuinely not be in storage, not merely reported failed.
	if got, err := backend.Get(ctx, "fenced/blocked"); err != nil {
		t.Fatalf("failed to read back: %v", err)
	} else if got != nil {
		t.Fatalf("fenced write still landed in storage: %q", got.Value)
	}

	// Deletes are fenced on the same basis.
	if err := backend.Delete(ctx, "fenced/ok"); !errors.Is(err, ErrLostActiveLock) {
		t.Fatalf("expected the delete to be fenced too, got: %v", err)
	}
	if got, err := backend.Get(ctx, "fenced/ok"); err != nil {
		t.Fatalf("failed to read back: %v", err)
	} else if got == nil {
		t.Fatal("fenced delete still removed the entry")
	}
}

// TestConsulBackend_Fencing_StepsDownAfterFencedWrite checks the node gives up
// leadership itself rather than waiting for the lock monitor to spend its
// retry budget noticing.
func TestConsulBackend_Fencing_StepsDownAfterFencedWrite(t *testing.T) {
	backend := newFencingBackend(t, fmt.Sprintf("test/openbao/fencing-stepdown-%d/", time.Now().UnixNano()))
	ctx := context.Background()

	lock, release := acquireActiveLock(t, backend, "core/lock", "node-a")
	defer release()

	_, session := lock.Info()
	if _, err := backend.client.Session().Destroy(session, nil); err != nil {
		t.Fatalf("failed to invalidate the session: %v", err)
	}

	_ = backend.Put(ctx, &physical.Entry{Key: "fenced/trigger", Value: []byte("x")})

	// Stepping down clears the lock's session, which is what core observes.
	if _, current := lock.Info(); current != "" {
		t.Errorf("expected the lock to have been released after a fenced write, still holds session %q", current)
	}
}

// TestConsulBackend_Fencing_UnregisteredWritesPass covers cluster
// initialization: the contract requires writes to work before any lock has
// been registered, since there is no active node yet.
func TestConsulBackend_Fencing_UnregisteredWritesPass(t *testing.T) {
	backend := newFencingBackend(t, fmt.Sprintf("test/openbao/fencing-init-%d/", time.Now().UnixNano()))
	ctx := context.Background()

	if backend.activeNodeLock.Load() != nil {
		t.Fatal("a freshly built backend must not have a registered lock")
	}
	if err := backend.Put(ctx, &physical.Entry{Key: "init/key", Value: []byte("v")}); err != nil {
		t.Fatalf("write before a lock is registered must succeed: %v", err)
	}
	if err := backend.Delete(ctx, "init/key"); err != nil {
		t.Fatalf("delete before a lock is registered must succeed: %v", err)
	}
}

// TestConsulBackend_Fencing_UnfencedWriteBypasses covers the other exemption
// the contract requires: writes explicitly marked unfenced, which let a sealed
// cluster be cleared and re-initialised even though a lock session exists.
func TestConsulBackend_Fencing_UnfencedWriteBypasses(t *testing.T) {
	backend := newFencingBackend(t, fmt.Sprintf("test/openbao/fencing-unfenced-%d/", time.Now().UnixNano()))
	ctx := context.Background()

	lock, release := acquireActiveLock(t, backend, "core/lock", "node-a")
	defer release()

	_, session := lock.Info()
	if _, err := backend.client.Session().Destroy(session, nil); err != nil {
		t.Fatalf("failed to invalidate the session: %v", err)
	}

	// A normal write is refused, but the same write marked unfenced lands.
	unfenced := physical.UnfencedWriteCtx(ctx)
	if err := backend.Put(unfenced, &physical.Entry{Key: "unfenced/key", Value: []byte("v")}); err != nil {
		t.Fatalf("unfenced write must bypass the session check: %v", err)
	}
	if got, err := backend.Get(ctx, "unfenced/key"); err != nil || got == nil {
		t.Fatalf("unfenced write did not land: entry=%v err=%v", got, err)
	}
}

// TestConsulBackend_Fencing_RegisterActiveNodeLock covers the registration
// contract itself.
func TestConsulBackend_Fencing_RegisterActiveNodeLock(t *testing.T) {
	backend := newFencingBackend(t, fmt.Sprintf("test/openbao/fencing-register-%d/", time.Now().UnixNano()))

	t.Run("rejects a foreign lock type", func(t *testing.T) {
		if err := backend.RegisterActiveNodeLock(nil); err == nil {
			t.Error("expected a lock of the wrong type to be rejected")
		}
	})

	t.Run("a session-less lock fails writes closed", func(t *testing.T) {
		// Never locked, so it has no session and cannot fence anything.
		// Registration is accepted -- the SDK contract gives no way to
		// decline, and callers do register already-released locks -- but the
		// write path must refuse rather than fall back to writing unfenced.
		fresh := newFencingBackend(t, fmt.Sprintf("test/openbao/fencing-nosession-%d/", time.Now().UnixNano()))
		l, err := fresh.LockWith("core/lock", "node-a")
		if err != nil {
			t.Fatalf("LockWith failed: %v", err)
		}
		if err := fresh.RegisterActiveNodeLock(l); err != nil {
			t.Fatalf("registration should be accepted: %v", err)
		}

		err = fresh.Put(context.Background(), &physical.Entry{Key: "nosession/key", Value: []byte("v")})
		if !errors.Is(err, ErrLostActiveLock) {
			t.Fatalf("expected writes to fail closed with no session to fence against, got: %v", err)
		}
	})

	t.Run("accepts a held lock and reports its session", func(t *testing.T) {
		lock, release := acquireActiveLock(t, backend, "core/lock", "node-a")
		defer release()

		key, session := lock.Info()
		if session == "" {
			t.Fatal("a held lock must expose the session backing it")
		}

		// The session must be the one Consul records against the key, or the
		// fencing check would be asserting against the wrong holder.
		pair, _, err := backend.kv.Get(key, nil)
		if err != nil || pair == nil {
			t.Fatalf("failed to read the lock key: pair=%v err=%v", pair, err)
		}
		if pair.Session != session {
			t.Errorf("lock reports session %q but Consul holds the key with %q", session, pair.Session)
		}
	})
}

// TestConsulBackend_Fencing_CheckIsFirstOp pins the ordering the failure
// handling depends on: the session check has to be operation zero, because
// that index is how a lost lock is told apart from an ordinary rejection.
func TestConsulBackend_Fencing_CheckIsFirstOp(t *testing.T) {
	backend := newFencingBackend(t, fmt.Sprintf("test/openbao/fencing-order-%d/", time.Now().UnixNano()))

	lock, release := acquireActiveLock(t, backend, "core/lock", "node-a")
	defer release()

	ops, session, err := backend.fencingOps(context.Background())
	if err != nil {
		t.Fatalf("fencingOps failed while holding the lock: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected exactly one fencing operation, got %d", len(ops))
	}
	if ops[0].KV.Verb != api.KVCheckSession {
		t.Errorf("expected a %s verb, got %s", api.KVCheckSession, ops[0].KV.Verb)
	}
	key, lockSession := lock.Info()
	if ops[0].KV.Key != key || ops[0].KV.Session != lockSession || session != lockSession {
		t.Errorf("fencing op checks %q/%q, lock holds %q/%q", ops[0].KV.Key, ops[0].KV.Session, key, lockSession)
	}
}

// TestConsulBackend_Fencing_ReleasedLockDoesNotUnfence guards the gap that
// closing one hole could open: once a lock has been registered and then
// released or lost, writes must fail rather than quietly falling back to an
// unfenced write. A node that stepped down going back to writing freely is the
// exact failure fencing exists to prevent.
func TestConsulBackend_Fencing_ReleasedLockDoesNotUnfence(t *testing.T) {
	backend := newFencingBackend(t, fmt.Sprintf("test/openbao/fencing-released-%d/", time.Now().UnixNano()))
	ctx := context.Background()

	lock, release := acquireActiveLock(t, backend, "core/lock", "node-a")

	// Give up the lock the way a graceful step-down does.
	release()
	if _, session := lock.Info(); session != "" {
		t.Fatalf("expected the released lock to report no session, got %q", session)
	}

	if _, _, err := backend.fencingOps(ctx); !errors.Is(err, ErrLostActiveLock) {
		t.Fatalf("expected a released lock to refuse fencing, got: %v", err)
	}

	if err := backend.Put(ctx, &physical.Entry{Key: "released/key", Value: []byte("v")}); !errors.Is(err, ErrLostActiveLock) {
		t.Fatalf("expected a write after release to be refused, got: %v", err)
	}
	if err := backend.Delete(ctx, "released/key"); !errors.Is(err, ErrLostActiveLock) {
		t.Fatalf("expected a delete after release to be refused, got: %v", err)
	}

	// The exemption still works, so a sealed cluster can still be cleared.
	if err := backend.Put(physical.UnfencedWriteCtx(ctx), &physical.Entry{Key: "released/unfenced", Value: []byte("v")}); err != nil {
		t.Fatalf("unfenced write must still bypass after release: %v", err)
	}
}

// TestConsulBackend_Fencing_AbandonedAcquisitionReleasesSession covers the
// cost of owning the session: Consul's lock helper only cleans up a session it
// created itself, and core does not call Unlock when acquisition is
// interrupted — it just drops the lock. Without explicit cleanup every
// abandoned attempt leaves a live session and a goroutine renewing it forever,
// and a standby retries acquisition repeatedly.
func TestConsulBackend_Fencing_AbandonedAcquisitionReleasesSession(t *testing.T) {
	path := fmt.Sprintf("test/openbao/fencing-abandon-%d/", time.Now().UnixNano())
	holder := newFencingBackend(t, path)
	contender := newFencingBackend(t, path)

	// One node holds the lock so the other cannot get it.
	_, release := acquireActiveLock(t, holder, "core/lock", "node-a")
	defer release()

	l, err := contender.LockWith("core/lock", "node-b")
	if err != nil {
		t.Fatalf("LockWith failed: %v", err)
	}
	lock := l.(*ConsulLock)

	// Build the session up front so its id can be captured; Lock reuses it.
	if _, err := lock.apiLock(); err != nil {
		t.Fatalf("failed to build the lock: %v", err)
	}
	_, abandoned := lock.Info()
	if abandoned == "" {
		t.Fatal("expected a session to have been created for the acquisition")
	}

	// Give up almost immediately, the way a shutdown during acquisition does.
	stopCh := make(chan struct{})
	timer := time.AfterFunc(100*time.Millisecond, func() { close(stopCh) })
	defer timer.Stop()

	leaderCh, err := lock.Lock(stopCh)
	if err != nil {
		t.Fatalf("an interrupted acquisition should not error: %v", err)
	}
	if leaderCh != nil {
		t.Fatal("acquisition should not have succeeded against a held lock")
	}

	// The session must be gone from Consul, not merely forgotten locally.
	if _, session := lock.Info(); session != "" {
		t.Errorf("abandoned lock still reports session %q", session)
	}
	entry, _, err := contender.client.Session().Info(abandoned, nil)
	if err != nil {
		t.Fatalf("failed to query the abandoned session: %v", err)
	}
	if entry != nil {
		t.Errorf("abandoned acquisition leaked consul session %q, which is still alive and being renewed", abandoned)
	}

	// And the lock must still be usable afterwards, since the SDK HA suite
	// re-locks the same object after an interrupted attempt.
	if _, err := lock.apiLock(); err != nil {
		t.Errorf("lock should remain reusable after an abandoned acquisition: %v", err)
	}
	if _, rebuilt := lock.Info(); rebuilt == "" || rebuilt == abandoned {
		t.Errorf("expected a fresh session on reuse, got %q (abandoned was %q)", rebuilt, abandoned)
	}
	lock.abandonSession()
}

// TestConsulBackend_Fencing_WriteFailureIsNotMistakenForLostLock guards the
// heuristic the failure handling rests on. The session check is operation
// zero, so only a failure at that index means leadership was lost; a rejected
// write must not be reported as a lost lock, and must not make the node step
// down.
func TestConsulBackend_Fencing_WriteFailureIsNotMistakenForLostLock(t *testing.T) {
	backend := newFencingBackend(t, fmt.Sprintf("test/openbao/fencing-index-%d/", time.Now().UnixNano()))
	ctx := context.Background()

	lock, release := acquireActiveLock(t, backend, "core/lock", "node-a")
	defer release()

	_, sessionBefore := lock.Info()
	if sessionBefore == "" {
		t.Fatal("expected a held lock to report a session")
	}

	// A delete op carrying an empty key is rejected by Consul at index 1,
	// after the session check at index 0 has passed.
	err := backend.runFencedTxn(ctx, "delete",
		&api.KVTxnOp{Verb: api.KVDelete, Key: ""},
		func() error { return nil })
	if err == nil {
		t.Fatal("expected the malformed write to be rejected")
	}
	if errors.Is(err, ErrLostActiveLock) {
		t.Errorf("a rejected write was misreported as a lost lock: %v", err)
	}

	// Leadership must be untouched: stepping down on an ordinary write error
	// would hand the cluster a needless failover.
	if _, sessionAfter := lock.Info(); sessionAfter != sessionBefore {
		t.Errorf("node stepped down over a write error: session went from %q to %q", sessionBefore, sessionAfter)
	}
}
