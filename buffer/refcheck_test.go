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
//     released via `defer ref.release()` registered on the same path as the
//     acquire. Two forms are accepted:
//     1. if-init:  if ref, ok := acquire(...); ok { defer ref.release() ... }
//     2. assign:   ref, ok := acquire(...); <guards>; defer ref.release()
//     Between the acquire and the defer, only "guards" are allowed: an if
//     whose condition references only `ok` and whose body exits without
//     touching the ref (e.g. `if !ok { return }`). Anything else — an early
//     return on another condition, a use of the ref, a conditional defer —
//     is a violation, because the defer might never be registered on a path
//     where the ref is live.
//   - test files: the pairing must still exist, but TestRefFileCloseWhenIdle
//     intentionally exercises a manual (non-deferred) release, so test files
//     only require that a release call is present.
//
// This is the enforcement mechanism for the refcount contract — a new call
// site that forgets release() (or registers it conditionally) fails this test
// in CI, so the mistake cannot slip in unnoticed.
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
			if !ok || fn.Body == nil {
				continue
			}
			checkBlock(fn, fn.Body.List, isTestFile, fset, &violations)
		}
	}

	if len(violations) > 0 {
		t.Errorf("refFile acquire/release contract violations:\n  %s\n"+
			"In non-test code, every acquire must be released with `defer ref.release()`\n"+
			"registered on the same path as the acquire (if-init form with condition `ok`,\n"+
			"or assign followed immediately by the defer, allowing only `if !ok { return }`\n"+
			"guards in between). Test code requires at least a release() call.",
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

// checkBlock walks a statement list, validating acquire/release pairing and
// recursing into nested blocks.
func checkBlock(fn *ast.FuncDecl, list []ast.Stmt, isTestFile bool, fset *token.FileSet, violations *[]string) {
	for i, stmt := range list {
		switch s := stmt.(type) {
		case *ast.AssignStmt:
			for j, rhs := range s.Rhs {
				call, ok := rhs.(*ast.CallExpr)
				if !ok || j >= len(s.Lhs) {
					continue
				}
				if !isAcquireCall(call) {
					continue
				}
				id, ok := s.Lhs[j].(*ast.Ident)
				if !ok {
					continue
				}
				if isTestFile {
					if !hasReleaseCall(fn, id.Name) {
						*violations = append(*violations, fmt.Sprintf("%s: acquire without release for %q", fset.Position(call.Pos()), id.Name))
					}
					continue
				}
				if !deferImmediatelyAfter(list, i+1, id.Name) {
					*violations = append(*violations, fmt.Sprintf("%s: acquire without immediately-following defer release for %q", fset.Position(call.Pos()), id.Name))
				}
			}
		case *ast.IfStmt:
			if s.Init != nil {
				if assign, ok := s.Init.(*ast.AssignStmt); ok {
					for j, rhs := range assign.Rhs {
						call, ok := rhs.(*ast.CallExpr)
						if !ok || j >= len(assign.Lhs) {
							continue
						}
						if !isAcquireCall(call) {
							continue
						}
						id, ok := assign.Lhs[j].(*ast.Ident)
						if !ok {
							continue
						}
						if isTestFile {
							if !hasReleaseCall(fn, id.Name) {
								*violations = append(*violations, fmt.Sprintf("%s: acquire without release for %q", fset.Position(call.Pos()), id.Name))
							}
						} else if !isValidIfInitForm(s, id.Name) {
							*violations = append(*violations, fmt.Sprintf("%s: if-init acquire for %q must have `ok` as condition and `defer %s.release()` as first body statement", fset.Position(call.Pos()), id.Name, id.Name))
						}
					}
				}
			}
			checkBlock(fn, s.Body.List, isTestFile, fset, violations)
			if s.Else != nil {
				checkStmt(fn, s.Else, isTestFile, fset, violations)
			}
		case *ast.ForStmt:
			checkBlock(fn, s.Body.List, isTestFile, fset, violations)
		case *ast.RangeStmt:
			checkBlock(fn, s.Body.List, isTestFile, fset, violations)
		case *ast.BlockStmt:
			checkBlock(fn, s.List, isTestFile, fset, violations)
		case *ast.SwitchStmt:
			for _, c := range s.Body.List {
				if cc, ok := c.(*ast.CaseClause); ok {
					checkBlock(fn, cc.Body, isTestFile, fset, violations)
				}
			}
		case *ast.TypeSwitchStmt:
			for _, c := range s.Body.List {
				if cc, ok := c.(*ast.CaseClause); ok {
					checkBlock(fn, cc.Body, isTestFile, fset, violations)
				}
			}
		case *ast.SelectStmt:
			for _, c := range s.Body.List {
				if cc, ok := c.(*ast.CommClause); ok {
					checkBlock(fn, cc.Body, isTestFile, fset, violations)
				}
			}
		case *ast.LabeledStmt:
			checkStmt(fn, s.Stmt, isTestFile, fset, violations)
		}
	}
}

// checkStmt dispatches a single statement into checkBlock for the statement
// kinds that can contain blocks (used for if/else chains and labels).
func checkStmt(fn *ast.FuncDecl, stmt ast.Stmt, isTestFile bool, fset *token.FileSet, violations *[]string) {
	switch s := stmt.(type) {
	case *ast.BlockStmt:
		checkBlock(fn, s.List, isTestFile, fset, violations)
	case *ast.IfStmt:
		checkBlock(fn, []ast.Stmt{s}, isTestFile, fset, violations)
	}
}

// deferImmediatelyAfter scans list[start:] for the release defer of varName,
// allowing only declarations and `if !ok { ...exit... }` guards in between.
func deferImmediatelyAfter(list []ast.Stmt, start int, varName string) bool {
	for i := start; i < len(list); i++ {
		stmt := list[i]
		if isDeferredReleaseStmt(stmt, varName) {
			return true
		}
		if isAllowedIntervening(stmt, varName) {
			continue
		}
		return false
	}
	return false
}

// isValidIfInitForm reports whether s has the form
// `if ref, ok := acquire(...); ok { defer ref.release() ... }`.
func isValidIfInitForm(s *ast.IfStmt, varName string) bool {
	cond, ok := s.Cond.(*ast.Ident)
	if !ok || cond.Name != "ok" {
		return false
	}
	if len(s.Body.List) == 0 {
		return false
	}
	return isDeferredReleaseStmt(s.Body.List[0], varName)
}

// isDeferredReleaseStmt reports whether stmt is `defer <varName>.release()`.
func isDeferredReleaseStmt(stmt ast.Stmt, varName string) bool {
	d, ok := stmt.(*ast.DeferStmt)
	if !ok {
		return false
	}
	return isReleaseCall(d.Call, varName)
}

// isReleaseCall reports whether call is `<varName>.release()`.
func isReleaseCall(call *ast.CallExpr, varName string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "release" {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == varName
}

// isAllowedIntervening reports whether stmt may sit between an acquire and its
// release defer: a declaration that does not touch the ref, or a guard of the
// form `if <condition over ok only> { <no ref use> ...exit }`.
func isAllowedIntervening(stmt ast.Stmt, varName string) bool {
	switch s := stmt.(type) {
	case *ast.DeclStmt:
		return !referencesIdent(s, varName)
	case *ast.IfStmt:
		// Guard: condition must mention ok (and nothing but ok), the body must
		// not touch the ref, and the body must end in an exit (return/continue/break).
		if referencesIdent(s.Cond, varName) {
			return false
		}
		if !isOkOnlyCond(s.Cond) {
			return false
		}
		if referencesIdent(s.Body, varName) {
			return false
		}
		return bodyEndsInExit(s.Body.List)
	case *ast.EmptyStmt:
		return true
	}
	return false
}

// isOkOnlyCond reports whether every identifier in cond is named "ok" and at
// least one is (so `if true { return }` is rejected).
func isOkOnlyCond(cond ast.Expr) bool {
	seenOk := false
	allOk := true
	ast.Inspect(cond, func(n ast.Node) bool {
		id, ok := n.(*ast.Ident)
		if !ok {
			return true
		}
		if id.Name == "ok" {
			seenOk = true
		} else {
			allOk = false
		}
		return true
	})
	return seenOk && allOk
}

// bodyEndsInExit reports whether the last statement of list is a return,
// continue or break.
func bodyEndsInExit(list []ast.Stmt) bool {
	if len(list) == 0 {
		return false
	}
	switch list[len(list)-1].(type) {
	case *ast.ReturnStmt, *ast.BranchStmt:
		return true
	}
	return false
}

// referencesIdent reports whether node contains an identifier named name.
func referencesIdent(node ast.Node, name string) bool {
	found := false
	ast.Inspect(node, func(n ast.Node) bool {
		if found {
			return false
		}
		if id, ok := n.(*ast.Ident); ok && id.Name == name {
			found = true
			return false
		}
		return true
	})
	return found
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
