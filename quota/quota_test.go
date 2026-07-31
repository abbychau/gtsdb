package quota

import (
	"gtsdb/auth"
	"os"
	"testing"
)

func setupAuth(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "gtsdb-quota-test")
	if err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	auth.Init(dir)
	return dir
}

func TestCheckWriteWithQuota(t *testing.T) {
	setupAuth(t)
	user, err := auth.CreateUserWithQuota("alice", 100)
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if user.MaxPoints != 100 {
		t.Fatalf("expected MaxPoints 100, got %d", user.MaxPoints)
	}

	if !CheckWrite("alice", 100) {
		t.Error("100 <= 100 should be allowed")
	}
	if !CheckWrite("alice", 50) {
		t.Error("50 should be allowed")
	}

	AddPoints("alice", 50)
	if !CheckWrite("alice", 50) {
		t.Error("50 (cached) + 50 = 100 should be allowed")
	}
	if CheckWrite("alice", 51) {
		t.Error("100 + 51 > 100 should be blocked")
	}
	if got := CurrentPoints("alice"); got != 50 {
		t.Errorf("expected cached points 50, got %d", got)
	}
}

func TestCheckWriteUnlimited(t *testing.T) {
	setupAuth(t)
	// root and users without a cap (0) are unlimited.
	if !CheckWrite("root", 1<<40) {
		t.Error("root should be unlimited")
	}
	if !CheckWrite("nobody", 12345) {
		t.Error("unknown user should not be blocked")
	}
}

func TestSetQuotaUpdatesUser(t *testing.T) {
	setupAuth(t)
	if _, err := auth.CreateUser("bob"); err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := auth.SetUserQuota("bob", 5000000); err != nil {
		t.Fatalf("set quota: %v", err)
	}
	u, ok := auth.GetUser("bob")
	if !ok || u.MaxPoints != 5000000 {
		t.Fatalf("expected bob quota 5000000, got %d (ok=%v)", u.MaxPoints, ok)
	}
}
