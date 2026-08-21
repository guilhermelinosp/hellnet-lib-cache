// Package golanglibtemplate is a minimal, idiomatic starter for Go libraries.
//
// It ships with a tiny, well-tested example API (Greet) so you can delete it
// and replace it with your own code without touching the surrounding tooling
// (CI, linter, pre-commit hooks, Makefile).
package golanglibtemplate

import "errors"

// Version is the library version. Bump it together with a release tag.
const Version = "0.1.0"

// ErrEmptyName is returned by Greet when name is empty.
var ErrEmptyName = errors.New("name is required")

// Greet returns a friendly greeting for name.
//
// It returns ErrEmptyName if name is empty.
func Greet(name string) (string, error) {
	if name == "" {
		return "", ErrEmptyName
	}
	return "Hello, " + name + "!", nil
}
