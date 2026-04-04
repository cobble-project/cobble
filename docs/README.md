# Cobble Docs

This directory hosts Cobble documentation using the `just-the-docs` Jekyll theme.

## Local preview

```bash
cd docs
bundle install
bundle exec jekyll serve
```

Then open `http://127.0.0.1:4000`.

## GitHub Pages deployment

- Workflow files (repo root):
  - `.github/workflows/docs-ci.yml`
  - `.github/workflows/docs-pages.yml`
- `docs-pages.yml` is pure workflow deployment (no manual script).
- In repository settings, enable **Pages -> Deploy from a branch** and point to `gh-pages` / `(root)`.

### Branch-to-root mapping

`docs-pages.yml` publishes by pushed branch name:

- `main` or `master` -> `latest/`
- numeric version branch -> same root
  - examples: `0.3.0`, `1.2`, `release/1.4.0` -> `0.3.0/`, `1.2/`, `1.4.0/`
- other branches are ignored by deploy job

### How to operate

1. Update docs in your target branch.
2. Push the branch to GitHub.
3. Workflow auto-builds and publishes to the corresponding root on `gh-pages`.

### Versioned roots

This repo deploys docs to versioned roots on the Pages branch:

- `latest/` (default target)
- `<semver>/` (for released docs, e.g. `0.1.0/`)

Example URLs:

- `https://cobble-project.github.io/cobble/latest/`
- `https://cobble-project.github.io/cobble/0.1.0/`
