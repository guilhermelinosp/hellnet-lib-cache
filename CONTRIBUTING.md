# Contributing

## Branch strategy

- `main` — stable, release-ready
- `feat/*` — new features
- `fix/*` — bug fixes
- `chore/*` — maintenance, dependencies, CI

## Commits

All commits must follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add new feature
fix: resolve login issue
docs: update README
refactor: simplify validation logic
chore: bump dependencies
```

The `release.yml` workflow uses commit messages to determine semver bumps.

## Pull Requests

- PR titles must also follow Conventional Commits
- Keep PRs focused — one feature/fix per PR
- Update tests and documentation as needed
- Ensure all CI checks pass before requesting review

## Issues

- Use the provided issue templates
- Include steps to reproduce for bugs
- Tag with appropriate labels
