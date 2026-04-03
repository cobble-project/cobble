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

- Workflow file: `docs/.github/workflows/pages.yml`
- Trigger:
  - push to `main` touching `docs/**` (deploys to `latest`)
  - manual `workflow_dispatch` (choose docs root + publish branch)
- In repository settings, enable **Pages -> Deploy from a branch** and point to `gh-pages` / `(root)`.

### Versioned roots

This repo deploys docs to versioned roots on the Pages branch:

- `latest/` (default target)
- `<semver>/` (for released docs, e.g. `0.1.0/`)

Example URLs:

- `https://cobble-project.github.io/cobble/latest/`
- `https://cobble-project.github.io/cobble/0.1.0/`

### Local/manual publish script

You can publish a specific root manually:

```bash
docs/scripts/deploy-versioned-pages.sh --root latest
docs/scripts/deploy-versioned-pages.sh --root 0.2.0 --publish-branch gh-pages
```
