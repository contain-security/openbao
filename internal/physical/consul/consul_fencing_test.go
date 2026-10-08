package consul

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
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

	// Deliberately last: this has to run with a real lock already registered.
	// atomic.Pointer.Load cannot tell "never stored" from "stored a nil", so
	// with nothing registered beforehand the not-stored assertion compares
	// nil against nil and holds however the code behaves. Storing a nil is
	// not a harmless slip either -- fencingOps reads it as "no lock
	// registered yet" and returns no check, silently unfencing every
	// subsequent write.
	t.Run("rejects a typed-nil lock", func(t *testing.T) {
		before := backend.activeNodeLock.Load()
		if before == nil {
			t.Fatal("this case is only meaningful once a real lock is registered")
		}
		if err := backend.RegisterActiveNodeLock((*ConsulLock)(nil)); err == nil {
			t.Error("expected a nil consul lock to be rejected")
		}
		if backend.activeNodeLock.Load() != before {
			t.Error("a rejected lock was stored anyway, which would unfence every later write")
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

// TestConsulBackend_Fencing_LegacyKeyConflictReleasesSession covers the error
// return from acquisition, as distinct from the interruption covered above.
//
// A lock key predating the move to Consul's lock helper carries no lock flag,
// so acquisition rejects it outright. Reclaiming is refused too, because this
// one is held by a live session, and the attempt ends in an error rather than
// a clean interruption. The session built for it must still be torn down, or a
// node retrying against such a key leaks one session and one renewal goroutine
// per attempt.
//
// The reclaim's own error branch releases the session through the same call,
// but is not reachable from a test without inducing a Consul failure mid-read,
// so it is covered by inspection rather than here.
func TestConsulBackend_Fencing_LegacyKeyConflictReleasesSession(t *testing.T) {
	path := fmt.Sprintf("test/openbao/fencing-reclaimfail-%d/", time.Now().UnixNano())
	backend := newFencingBackend(t, path)

	// Seed a legacy-format key: no flags, and held by a session, which is the
	// combination the reclaim refuses to touch. Acquisition then cannot
	// succeed and cannot reclaim.
	key := "core/lock"
	holderSession, _, err := backend.client.Session().Create(&api.SessionEntry{
		Name:     "openbao-legacy-holder",
		TTL:      "120s",
		Behavior: api.SessionBehaviorRelease,
	}, nil)
	if err != nil {
		t.Fatalf("failed to create the holding session: %v", err)
	}
	t.Cleanup(func() { _, _ = backend.client.Session().Destroy(holderSession, nil) })

	acquired, _, err := backend.kv.Acquire(&api.KVPair{
		Key:     backend.consulKey(key),
		Value:   []byte("legacy"),
		Session: holderSession,
	}, nil)
	if err != nil || !acquired {
		t.Fatalf("failed to seed the legacy key: acquired=%v err=%v", acquired, err)
	}

	l, err := backend.LockWith(key, "node-b")
	if err != nil {
		t.Fatalf("LockWith failed: %v", err)
	}
	lock := l.(*ConsulLock)

	if _, err := lock.apiLock(); err != nil {
		t.Fatalf("failed to build the lock: %v", err)
	}
	_, session := lock.Info()
	if session == "" {
		t.Fatal("expected a session to have been created for the acquisition")
	}

	stopCh := make(chan struct{})
	timer := time.AfterFunc(30*time.Second, func() { close(stopCh) })
	defer timer.Stop()

	leaderCh, lockErr := lock.Lock(stopCh)
	if leaderCh != nil {
		t.Fatal("acquisition should not succeed against a held legacy key")
	}
	if lockErr == nil {
		t.Fatal("expected acquisition against a held legacy key to fail")
	}

	// Whatever the failure, the session must not outlive it.
	entry, _, err := backend.client.Session().Info(session, nil)
	if err != nil {
		t.Fatalf("failed to query the session: %v", err)
	}
	if entry != nil {
		t.Errorf("failed acquisition leaked consul session %q", session)
	}
	if _, remaining := lock.Info(); remaining != "" {
		t.Errorf("abandoned lock still reports session %q", remaining)
	}
}

// TestConsulBackend_Fencing_HeldLockKeepsSessionOnRelock guards the other
// direction: abandoning a session on any acquisition error would destroy the
// session of a lock this node is actively holding, turning a caller mistake
// into a needless failover.
func TestConsulBackend_Fencing_HeldLockKeepsSessionOnRelock(t *testing.T) {
	backend := newFencingBackend(t, fmt.Sprintf("test/openbao/fencing-relock-%d/", time.Now().UnixNano()))

	lock, release := acquireActiveLock(t, backend, "core/lock", "node-a")
	defer release()

	_, session := lock.Info()
	if session == "" {
		t.Fatal("expected a held lock to report a session")
	}

	// Locking an already-held lock is a caller error, not a lost lock.
	stopCh := make(chan struct{})
	defer close(stopCh)
	if _, err := lock.Lock(stopCh); err == nil {
		t.Fatal("expected re-locking a held lock to fail")
	}

	if _, after := lock.Info(); after != session {
		t.Errorf("session changed from %q to %q: a held lock was torn down", session, after)
	}
	entry, _, err := backend.client.Session().Info(session, nil)
	if err != nil {
		t.Fatalf("failed to query the session: %v", err)
	}
	if entry == nil {
		t.Error("the session of a lock this node still holds was destroyed")
	}
}

// TestConsulBackend_Fencing_TakeoverRefusesFormerLeaderWrites is the scenario
// the whole mechanism exists for, played out with two real nodes rather than
// simulated by hand.
//
// Node A holds the lock and is writing. Its session lapses — a partition, a
// paused process, a Consul it briefly cannot reach — and node B, already
// waiting, acquires the lock and starts writing. A has not yet noticed. Every
// write A issues from that moment must be refused, and B's must succeed.
//
// The other tests in this file destroy the session directly to reach the same
// internal state. This one lets Consul hand leadership over on its own, so it
// also covers the ordering: B genuinely holds the lock at the instant A's
// writes are rejected.
func TestConsulBackend_Fencing_TakeoverRefusesFormerLeaderWrites(t *testing.T) {
	path := fmt.Sprintf("test/openbao/fencing-takeover-%d/", time.Now().UnixNano())

	// A short lock delay so the successor can take over without waiting out
	// the default. The delay is a safety margin for exactly this transition;
	// shortening it here narrows the window rather than removing the check.
	newNode := func() *ConsulBackend {
		t.Helper()
		cfg := requireConsul(t, path)
		cfg["ha_enabled"] = "true"
		cfg["lock_delay"] = "1ms"
		b, err := NewConsulBackend(cfg, hclog.NewNullLogger())
		if err != nil {
			failOrSkip(t, "Consul not available: %v", err)
		}
		return b.(*ConsulBackend)
	}

	nodeA, nodeB := newNode(), newNode()

	// A becomes active.
	lockA, releaseA := acquireActiveLock(t, nodeA, "core/lock", "node-a")
	defer releaseA()

	if err := nodeA.Put(context.Background(), &physical.Entry{Key: "shared/key", Value: []byte("from-a")}); err != nil {
		t.Fatalf("the active node should be able to write: %v", err)
	}

	// B waits for the lock, as a standby does.
	lB, err := nodeB.LockWith("core/lock", "node-b")
	if err != nil {
		t.Fatalf("LockWith failed: %v", err)
	}
	lockB := lB.(*ConsulLock)

	stopB := make(chan struct{})
	defer close(stopB)
	acquired := make(chan error, 1)
	go func() {
		leaderCh, err := lockB.Lock(stopB)
		if err == nil && leaderCh == nil {
			err = fmt.Errorf("acquisition was interrupted before node B became active")
		}
		acquired <- err
	}()

	// A's session lapses. Consul releases the key, and B's pending
	// acquisition wins it.
	_, sessionA := lockA.Info()
	if _, err := nodeA.client.Session().Destroy(sessionA, nil); err != nil {
		t.Fatalf("failed to invalidate node A's session: %v", err)
	}

	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("node B failed to take over: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for node B to take over the lock")
	}

	// Registered here because this is the first point where nothing else
	// would free B's session: an acquisition that fails or is interrupted
	// above is already cleaned up by abandonSession, whereas from here on
	// only an explicit release ends it, and a failure below would otherwise
	// leave it renewing and holding the key for the rest of the run. Once
	// the explicit release further down has run this becomes a no-op, so it
	// neither masks nor duplicates the call that asserts a release succeeds.
	defer func() { _ = lockB.Unlock() }()

	if err := nodeB.RegisterActiveNodeLock(lockB); err != nil {
		t.Fatalf("node B failed to register its lock: %v", err)
	}

	// B genuinely holds it.
	keyB, sessionB := lockB.Info()
	pair, _, err := nodeB.kv.Get(keyB, nil)
	if err != nil || pair == nil {
		t.Fatalf("failed to read the lock key: pair=%v err=%v", pair, err)
	}
	if pair.Session != sessionB {
		t.Fatalf("expected node B to hold the lock with %q, Consul reports %q", sessionB, pair.Session)
	}

	ctx := context.Background()

	// The successor writes normally.
	if err := nodeB.Put(ctx, &physical.Entry{Key: "shared/key", Value: []byte("from-b")}); err != nil {
		t.Fatalf("the new active node should be able to write: %v", err)
	}

	// Pin the path under test: node A still believes it holds the lock, so
	// the next operation carries a real session check to Consul rather than
	// stopping at the local guard. Only the first refused operation reaches
	// Consul -- refusing it steps the node down, which clears the session --
	// so the destructive verb goes first, and the delete assertion below
	// says "by consul" to record which route it is meant to take. These
	// checks assert the transition, not the ordering: swapping the two verbs
	// would still satisfy both, so the delete-through-Consul property rests
	// on that ordering being kept.
	if _, current := lockA.Info(); current != sessionA {
		t.Fatalf("node A should still believe it holds the lock with %q, reports %q", sessionA, current)
	}

	if err := nodeA.Delete(ctx, "shared/key"); !errors.Is(err, ErrLostActiveLock) {
		t.Fatalf("the former leader's delete was not refused by consul: %v", err)
	}

	// Now stepped down, so this one is refused locally. Both routes matter:
	// a demoted node must not write whether or not it has noticed yet.
	if _, current := lockA.Info(); current != "" {
		t.Fatalf("expected node A to have stepped down, still holds %q", current)
	}
	err = nodeA.Put(ctx, &physical.Entry{Key: "shared/key", Value: []byte("from-a-after-takeover")})
	if !errors.Is(err, ErrLostActiveLock) {
		t.Fatalf("the former leader's write was not refused: %v", err)
	}

	// And the successor's value is what is actually in storage: the former
	// leader neither overwrote nor removed it.
	got, err := nodeB.Get(ctx, "shared/key")
	if err != nil {
		t.Fatalf("failed to read back: %v", err)
	}
	if got == nil {
		t.Fatal("the former leader's delete removed the successor's write")
	}
	if string(got.Value) != "from-b" {
		t.Fatalf("storage holds %q: the former leader overwrote the active node's data", got.Value)
	}

	// Losing the lock must not be terminal. A node fenced out and left fenced
	// forever is an availability failure as total as the corruption this
	// guards against, and nothing else in the suite would notice it.
	if err := lockB.Unlock(); err != nil {
		t.Fatalf("node B failed to release the lock: %v", err)
	}

	lA2, err := nodeA.LockWith("core/lock", "node-a-again")
	if err != nil {
		t.Fatalf("LockWith failed: %v", err)
	}
	lockA2 := lA2.(*ConsulLock)
	// Bounded like every other blocking acquire here, so a hang fails this
	// test rather than panicking the whole package on the global timeout.
	stopA2 := make(chan struct{})
	a2Timer := time.AfterFunc(60*time.Second, func() { close(stopA2) })
	defer a2Timer.Stop()

	leaderChA2, err := lockA2.Lock(stopA2)
	if err != nil {
		t.Fatalf("node A failed to re-acquire the lock: %v", err)
	}
	if leaderChA2 == nil {
		t.Fatal("node A's re-acquisition was interrupted")
	}
	defer func() { _ = lockA2.Unlock() }()

	if err := nodeA.RegisterActiveNodeLock(lockA2); err != nil {
		t.Fatalf("node A failed to register its new lock: %v", err)
	}

	// Fenced against the new session, not the dead one.
	_, sessionA2 := lockA2.Info()
	if sessionA2 == "" || sessionA2 == sessionA {
		t.Fatalf("expected a fresh session on re-acquisition, got %q (original was %q)", sessionA2, sessionA)
	}
	ops, fencedWith, err := nodeA.fencingOps(ctx)
	if err != nil {
		t.Fatalf("fencingOps failed after re-acquisition: %v", err)
	}
	if len(ops) != 1 || fencedWith != sessionA2 {
		t.Fatalf("writes should now be fenced against %q, got %q from %d ops", sessionA2, fencedWith, len(ops))
	}

	if err := nodeA.Put(ctx, &physical.Entry{Key: "shared/key", Value: []byte("from-a-again")}); err != nil {
		t.Fatalf("node A should be able to write again once it regains the lock: %v", err)
	}
	if got, err := nodeA.Get(ctx, "shared/key"); err != nil || got == nil || string(got.Value) != "from-a-again" {
		t.Fatalf("re-acquired node's write did not land: entry=%v err=%v", got, err)
	}
}

// TestConsulBackend_Fencing_StepDownGuards covers which sessions a step-down
// will and will not act on.
//
// A fenced write can fail slowly enough that this node has since re-acquired
// the lock under a new session. Stepping down on the old session's behalf
// would give up leadership the node legitimately holds, so the session is
// re-read and compared first. Both cases use a genuinely held lock: a
// hand-built one is not enough, because Unlock returns early when no lock was
// ever acquired and would leave the asserted fields untouched no matter what
// the guard did.
func TestConsulBackend_Fencing_StepDownGuards(t *testing.T) {
	t.Run("no registered lock", func(t *testing.T) {
		backend := &ConsulBackend{logger: hclog.NewNullLogger(), path: "test/"}
		// Nothing to release, and nothing to panic on.
		backend.stepDown("some-session")
	})

	t.Run("a superseded session is ignored", func(t *testing.T) {
		backend := newFencingBackend(t, fmt.Sprintf("test/openbao/fencing-guard-stale-%d/", time.Now().UnixNano()))
		lock, release := acquireActiveLock(t, backend, "core/lock", "node-a")
		defer release()

		key, session := lock.Info()
		backend.stepDown("a-session-this-node-no-longer-uses")

		if _, current := lock.Info(); current != session {
			t.Errorf("a step-down for a superseded session released the current lock: session went from %q to %q", session, current)
		}
		// And Consul must still record the node as the holder.
		pair, _, err := backend.kv.Get(key, nil)
		if err != nil || pair == nil {
			t.Fatalf("failed to read the lock key: pair=%v err=%v", pair, err)
		}
		if pair.Session != session {
			t.Errorf("consul no longer records the lock as held by %q, it holds %q", session, pair.Session)
		}
	})

	t.Run("the current session steps down", func(t *testing.T) {
		backend := newFencingBackend(t, fmt.Sprintf("test/openbao/fencing-guard-current-%d/", time.Now().UnixNano()))
		lock, release := acquireActiveLock(t, backend, "core/lock", "node-a")
		defer release()

		key, session := lock.Info()
		backend.stepDown(session)

		if _, current := lock.Info(); current != "" {
			t.Errorf("expected the lock to be released, still holds %q", current)
		}
		pair, _, err := backend.kv.Get(key, nil)
		if err != nil {
			t.Fatalf("failed to read the lock key: %v", err)
		}
		if pair != nil && pair.Session != "" {
			t.Errorf("consul still records the key as held by %q after stepping down", pair.Session)
		}
	})
}

// TestConsulBackend_MapWriteError covers the size mapping, so an oversized
// value is reported as such rather than as an opaque failure, and is not
// retried. Needs no Consul.
func TestConsulBackend_MapWriteError(t *testing.T) {
	backend := &ConsulBackend{logger: hclog.NewNullLogger()}

	cases := []struct {
		name      string
		err       error
		wantMap   bool
		wantRetry bool
	}{
		{name: "nil stays nil", err: nil},
		{
			name:    "consul size rejection",
			err:     errors.New("Value exceeds 524288 byte limit: value is too large"),
			wantMap: true,
		},
		{
			name:    "transaction size rejection",
			err:     errors.New("Request body(600000 bytes) too large, max size: 524288 bytes"),
			wantMap: true,
		},
		{
			name:      "an unrelated failure is untouched and still retryable",
			err:       errors.New("connection refused"),
			wantRetry: true,
		},
		{
			// Deliberately terminal: a demoted node must stop writing at
			// once rather than burn the retry budget.
			name: "a lost lock is terminal",
			err:  fmt.Errorf("write: %w", ErrLostActiveLock),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mapWriteError(tc.err)
			if tc.err == nil {
				if got != nil {
					t.Fatalf("expected nil, got %v", got)
				}
				return
			}
			if mapped := strings.Contains(got.Error(), physical.ErrValueTooLarge); mapped != tc.wantMap {
				t.Errorf("expected mapped=%v, got %q", tc.wantMap, got)
			}
			// The original text has to survive, or the cause is lost.
			if !strings.Contains(got.Error(), tc.err.Error()) {
				t.Errorf("original error text lost: %q", got)
			}
			if retry := backend.isRetryableError(got); retry != tc.wantRetry {
				t.Errorf("expected retryable=%v for %q", tc.wantRetry, got)
			}
		})
	}
}

// TestConsulBackend_Fencing_ConcurrentWritesDuringTakeover runs many writes in
// parallel across the moment the lock is lost.
//
// The step-down happens on whichever write notices first, while others are
// still in flight, so this is where a race or a double-release would show. The
// property asserted is that no write is silently accepted after the lock is
// gone: each either committed before the loss or was refused.
func TestConsulBackend_Fencing_ConcurrentWritesDuringTakeover(t *testing.T) {
	backend := newFencingBackend(t, fmt.Sprintf("test/openbao/fencing-concurrent-%d/", time.Now().UnixNano()))
	ctx := context.Background()

	lock, release := acquireActiveLock(t, backend, "core/lock", "node-a")
	defer release()

	const writers = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([]error, writers)

	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = backend.Put(ctx, &physical.Entry{
				Key:   fmt.Sprintf("concurrent/key-%02d", i),
				Value: []byte("v"),
			})
		}(i)
	}

	// Pull the lock out from under them all at once.
	_, session := lock.Info()
	close(start)
	if _, err := backend.client.Session().Destroy(session, nil); err != nil {
		t.Fatalf("failed to invalidate the session: %v", err)
	}
	wg.Wait()

	// Every write either landed while the lock was still held, or was
	// refused. Anything else means a write slipped through unfenced.
	for i, err := range results {
		if err == nil {
			continue
		}
		if !errors.Is(err, ErrLostActiveLock) {
			t.Errorf("writer %d failed for an unexpected reason: %v", i, err)
		}
	}

	// The step-down only happens on a write that was actually fenced, and the
	// race admits an interleaving where all 16 commit before the session is
	// destroyed. One further write makes the outcome deterministic rather
	// than asserting a property the setup does not guarantee.
	if err := backend.Put(ctx, &physical.Entry{Key: "concurrent/after", Value: []byte("v")}); !errors.Is(err, ErrLostActiveLock) {
		t.Fatalf("a write after the session was destroyed must be refused, got: %v", err)
	}
	if _, remaining := lock.Info(); remaining != "" {
		t.Errorf("expected the lock to have been released after losing the session, still holds %q", remaining)
	}

	// And storage must agree with what the writes reported. The refused-but-
	// present direction assumes a write reports failure only if it did not
	// commit, which holds here but is not guaranteed in general: a
	// transaction that commits and then loses its response is retried, and
	// the retry is fenced, so the caller sees a refusal for a write that
	// landed. Against a local agent that has not been observed.
	for i, err := range results {
		key := fmt.Sprintf("concurrent/key-%02d", i)
		got, getErr := backend.Get(ctx, key)
		if getErr != nil {
			t.Fatalf("failed to read back %s: %v", key, getErr)
		}
		if err == nil && got == nil {
			t.Errorf("writer %d reported success but %s is absent", i, key)
		}
		if err != nil && got != nil {
			t.Errorf("writer %d was refused but %s landed anyway", i, key)
		}
	}
}
