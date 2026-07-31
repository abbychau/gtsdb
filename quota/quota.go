// Package quota enforces per-user max data point storage WITHOUT hurting the
// write/read hot path.
//
// Design:
//   - A cached per-user counter is maintained in memory (O(1) atomic load).
//   - Writes do a single O(1) check (cached + incoming <= max) and one atomic
//     add on success. No scanning on the write path.
//   - A background reconciler (every 5 minutes by default) recomputes each
//     user's total from buffer.GetAllIdsWithCount() and corrects drift from
//     deletes / compactions / patches. So enforcement is near-real-time for
//     writes, and accurate within one reconcile interval overall.
package quota

import (
	"strings"
	"sync/atomic"
	"time"

	"gtsdb/auth"
	"gtsdb/buffer"
	"gtsdb/concurrent"
	"gtsdb/utils"
)

// userPoints caches each user's current stored data points (their own
// namespace only, i.e. keys prefixed "<user>/"). Values are updated
// incrementally on writes and replaced by Reconcile with the authoritative
// counts read from the buffer.
var userPoints = concurrent.NewMap[string, *atomic.Int64]()

func pointsFor(name string) *atomic.Int64 {
	if p, ok := userPoints.Load(name); ok {
		return p
	}
	p := &atomic.Int64{}
	if existing, loaded := userPoints.LoadOrStore(name, p); loaded {
		return existing
	}
	return p
}

// userFromKey maps a fully-qualified key to its owning user. Keys are always
// prefixed "<user>/"; legacy unprefixed keys belong to the shared root folder.
func userFromKey(key string) string {
	if idx := strings.IndexByte(key, '/'); idx > 0 {
		return key[:idx]
	}
	return "root"
}

// MaxPoints returns a user's configured storage cap (0 = unlimited).
func MaxPoints(name string) int64 {
	u, ok := auth.GetUser(name)
	if !ok {
		return 0
	}
	return u.MaxPoints
}

// CheckWrite reports whether writing `incoming` more points is allowed for the
// user. O(1): reads the user's cap and the cached counter. Unlimited users
// (root, no cap) always pass.
func CheckWrite(name string, incoming int64) bool {
	if incoming <= 0 {
		return true
	}
	max := MaxPoints(name)
	if max <= 0 {
		return true
	}
	return pointsFor(name).Load()+incoming <= max
}

// AddPoints records `n` points written for the user (call after a successful
// write). O(1) atomic add.
func AddPoints(name string, n int64) {
	if n <= 0 {
		return
	}
	pointsFor(name).Add(n)
}

// CurrentPoints returns the cached point count for a user (for observability).
func CurrentPoints(name string) int64 {
	if p, ok := userPoints.Load(name); ok {
		return p.Load()
	}
	return 0
}

// Reconcile recomputes every user's total from the buffer and replaces the
// cached counters. Called periodically off the hot path.
func Reconcile() {
	keyCounts := buffer.GetAllIdsWithCount()
	fresh := make(map[string]int64, 32)
	for _, kc := range keyCounts {
		fresh[userFromKey(kc.Key)] += int64(kc.Count)
	}

	for name, val := range fresh {
		pointsFor(name).Store(val)
	}
	// Drop cached entries for users that no longer hold any data.
	userPoints.Range(func(name string, _ *atomic.Int64) bool {
		if _, ok := fresh[name]; !ok {
			userPoints.Delete(name)
		}
		return true
	})
}

// StartReconciler runs Reconcile immediately, then every `interval` until
// `stop` is closed. interval is clamped to >= 1 minute to avoid hot-looping.
func StartReconciler(interval time.Duration, stop <-chan struct{}) {
	if interval < time.Minute {
		interval = 5 * time.Minute
	}
	go func() {
		Reconcile()
		utils.Log("[quota] reconciler started (interval %v)", interval)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				Reconcile()
			}
		}
	}()
}
