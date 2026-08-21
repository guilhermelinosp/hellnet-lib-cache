# golang-lib-template

> GitHub template for production-ready Go libraries. Pre-configured with CI,
> linting, pre-commit hooks, and dependency automation.

Click **"Use this template"** to scaffold a new Go library in seconds.

## What's included

- **Go module** seeded with a tiny, tested example API (`Greet`) — delete it and start coding.
- **`.golangci.yml`** — curated linter config (errcheck, staticcheck, gosec, revive, …).
- **`Makefile`** — `fmt`, `vet`, `lint`, `test`, `test-race`, `cover`, `build`.
- **Lefthook** pre-commit hooks (`.lefthook.yml`): `go fmt`, `go vet`, `go mod tidy`,
  `golangci-lint`, `yamllint`, `gitleaks`. `-race` tests run on pre-push.
- **CI** (`.github/workflows`):
  - `pipeline.yml` (main): semantic release + Go build via [ci-templates](https://github.com/guilhermelinosp/ci-templates).
  - `pr-check.yml` (PR): Go vet + `go test -race` + `golangci-lint`, plus shellcheck, gitleaks, merge-check, labeler.
- **Dependency automation** via Dependabot (`github-actions` + `gomod`).
- **Repo meta**: issue/PR/discussion templates, `CODEOWNERS`, `SECURITY.md`, `CONTRIBUTING.md`, `FUNDING.yml`.

## Quick start

```bash
# 1. create your repo from this template, then:
go mod edit -module github.com/<you>/<repo>   # replace the module path
go get ./...
```

```go
package main

import (
	"fmt"

	"github.com/<you>/<repo>"
)

func main() {
	msg, err := <repo>.Greet("World")
	if err != nil {
		panic(err)
	}
	fmt.Println(msg) // Hello, World!
}
```

## Develop

```bash
make all        # fmt + vet + lint + test
make test-race  # tests with the race detector
make cover      # coverage report (coverage.out)
```

Install the git hooks once:

```bash
lefthook install
```

## Conventional Commits

Commits and PR titles follow [Conventional Commits](https://www.conventionalcommits.org/).
Releases and version bumps are derived from them — see `CONTRIBUTING.md`.

## License

[Apache 2.0](LICENSE)
