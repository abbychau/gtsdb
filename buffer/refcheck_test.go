package buffer

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAcquireReleasePairing statically verifies the refFile acquire/release
// contract on this package's own source code:
//   - non-test files: every acquireFileHandle/refFromLRU result must be
//     released via `defer ref.release()` — defer guarantees the release runs
//     on EVERY path out of the function (early returns, panics), which a bare
//     ref.release() call cannot. A plain call could sit behind a conditional
//     or an early return and silently leak.
//   - test files: the pairing must still exist, but TestRefFileCloseWhenIdle
//     intentionally exercises a manual (non-deferred) release, so test files
//     only require that a release call is present.
//
// This is the enforcement mechanism for the refcount contract — a new call
// site that forgets release() (or releases without defer) fails this test in
// CI, so the mistake cannot slip in unnoticed.
func TestAcquireReleasePairing(t *testing.T) {
	srcFiles, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}

	var violations []string
	for _, name := range srcFiles {
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		isTestFile := strings.HasSuffix(name, "_test.go")

		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				assign, ok := n.(*ast.AssignStmt)
				if !ok {
					return true
				}
				for i, rhs := range assign.Rhs {
					call, ok := rhs.(*ast.CallExpr)
					if !ok || i >= len(assign.Lhs) {
						continue
					}
					if !isAcquireCall(call) {
						continue
					}
					id, ok := assign.Lhs[i].(*ast.Ident)
					if !ok {
						continue
					}
					var ok2 bool
					if isTestFile {
						ok2 = hasReleaseCall(fn, id.Name)
					} else {
						ok2 = hasDeferredRelease(fn, id.Name)
					}
					if !ok2 {
						pos := fset.Position(call.Pos())
						violations = append(violations, fmt.Sprintf("%s: acquire without %s for %q", pos,
							map[bool]string{true: "release", false: "deferred release"}[isTestFile], id.Name))
					}
				}
				return true
			})
		}
	}

	if len(violations) > 0 {
		t.Errorf("refFile acquire/release contract violations:\n  %s\n"+
			"Every acquireFileHandle/refFromLRU result in non-test code must be released with\n"+
			"`defer ref.release()` right after the acquire, so the release runs on every path.\n"+
			"Test code requires at least a release() call (manual releases allowed for testing\n"+
			"the close semantics directly).",
			strings.Join(violations, "\n  "))
	}
}

// isAcquireCall reports whether call is a call to acquireFileHandle or
// refFromLRU (same-package calls, so the callee is a plain identifier).
func isAcquireCall(call *ast.CallExpr) bool {
	id, ok := call.Fun.(*ast.Ident)
	if !ok {
		return false
	}
	return id.Name == "acquireFileHandle" || id.Name == "refFromLRU"
}

// hasReleaseCall reports whether fn contains a release() method call whose
// receiver is a variable named varName (e.g. `ref.release()` or
// `defer ref.release()`).
func hasReleaseCall(fn *ast.FuncDecl, varName string) bool {
	found := false
	ast.Inspect(fn, func(n ast.Node) bool {
		if found {
			return false
		}
		sel, ok := n.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "release" {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == varName {
			found = true
			return false
		}
		return true
	})
	return found
}

// hasDeferredRelease reports whether fn contains `defer ref.release()` for a
// variable named varName. Deferring is what guarantees the release runs on
// every path out of the function.
func hasDeferredRelease(fn *ast.FuncDecl, varName string) bool {
	found := false
	ast.Inspect(fn, func(n ast.Node) bool {
		if found {
			return false
		}
		d, ok := n.(*ast.DeferStmt)
		if !ok {
			return true
		}
		call := d.Call
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "release" {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == varName {
			found = true
			return false
		}
		return true
	})
	return found
}
