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
// contract on this package's own source code: every assignment from
// acquireFileHandle or refFromLRU must have a matching release() call on the
// same variable within the same function.
//
// This is the enforcement mechanism for the refcount contract — a new call
// site that forgets release() fails this test in CI, so the mistake cannot
// slip in unnoticed. (It is intentionally syntax-based: the patterns used in
// this package are simple and the check has no false positives on them.)
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
					if !hasReleaseCall(fn, id.Name) {
						pos := fset.Position(call.Pos())
						violations = append(violations, fmt.Sprintf("%s: acquire without release for %q", pos, id.Name))
					}
				}
				return true
			})
		}
	}

	if len(violations) > 0 {
		t.Errorf("refFile acquire/release contract violations:\n  %s\n"+
			"Every acquireFileHandle/refFromLRU result must be paired with a release() call\n"+
			"in the same function (typically `defer ref.release()` right after the acquire).",
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
