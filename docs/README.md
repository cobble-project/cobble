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

### Trigger and root mapping

`docs-pages.yml` only publishes in these two cases:

1. Push to `main` / `master` **and** this push contains changes under `docs/**` -> publish `latest/`
2. Push a version tag with `v` prefix (e.g. `v0.3.0`) -> publish `<version>/` (e.g. `0.3.0/`)

All other pushes/tags are ignored by deploy job.

### How to operate

1. For `latest/`: update docs and push to `main` / `master`.
2. For versioned docs: create and push a version tag (e.g. `v0.3.0`).
3. Workflow auto-builds and publishes to corresponding root on `gh-pages`.

### Versioned roots

This repo deploys docs to versioned roots on the Pages branch:

- `latest/` (default target)
- `<semver>/` (for released docs, e.g. `0.1.0/`)

Example URLs:

- `https://cobble-project.github.io/cobble/latest/`
- `https://cobble-project.github.io/cobble/0.1.0/`
