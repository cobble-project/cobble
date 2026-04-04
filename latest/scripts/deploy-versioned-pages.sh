#!/usr/bin/env bash
set -euo pipefail

ROOT="latest"
PUBLISH_BRANCH="gh-pages"
REMOTE="origin"
SITE_BASE=""
SKIP_BUILD=0

usage() {
  cat <<'EOF'
Usage: deploy-versioned-pages.sh [options]

Build docs and publish them into a versioned root on the pages branch.

Options:
  --root <name>             Docs root path (default: latest)
                            Examples: latest, 0.1.0, 1.2.3
  --publish-branch <name>   Target branch to publish static files (default: gh-pages)
  --remote <name>           Git remote name (default: origin)
  --site-base <path>        Site base prefix used for Jekyll --baseurl (default: /<repo-name>)
  --skip-build              Skip Jekyll build and publish existing docs/_site
  -h, --help                Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root)
      ROOT="${2:-}"
      shift 2
      ;;
    --publish-branch)
      PUBLISH_BRANCH="${2:-}"
      shift 2
      ;;
    --remote)
      REMOTE="${2:-}"
      shift 2
      ;;
    --site-base)
      SITE_BASE="${2:-}"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "${ROOT}" ]]; then
  echo "--root cannot be empty" >&2
  exit 2
fi
if [[ "${ROOT}" == *"/"* || "${ROOT}" == "." || "${ROOT}" == ".." ]]; then
  echo "--root must be a single path segment, got: ${ROOT}" >&2
  exit 2
fi
if [[ "${ROOT}" != "latest" && ! "${ROOT}" =~ ^[0-9]+(\.[0-9]+){1,2}([.-][0-9A-Za-z._-]+)?$ ]]; then
  echo "--root must be 'latest' or a version-like value (e.g. 1.2.3), got: ${ROOT}" >&2
  exit 2
fi

REPO_ROOT="$(git rev-parse --show-toplevel)"
DOCS_DIR="${REPO_ROOT}/docs"
SITE_DIR="${DOCS_DIR}/_site"

if [[ ! -d "${DOCS_DIR}" ]]; then
  echo "docs directory not found at ${DOCS_DIR}" >&2
  exit 1
fi

if [[ -z "${SITE_BASE}" ]]; then
  REPO_NAME="$(basename "${REPO_ROOT}")"
  SITE_BASE="/${REPO_NAME}"
fi
SITE_BASE="/${SITE_BASE#/}"
SITE_BASE="${SITE_BASE%/}"
BASEURL="${SITE_BASE}/${ROOT}"

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  (
    cd "${DOCS_DIR}"
    bundle exec jekyll build --strict_front_matter --baseurl "${BASEURL}"
  )
fi

if [[ ! -d "${SITE_DIR}" ]]; then
  echo "Built site not found at ${SITE_DIR}" >&2
  exit 1
fi

REMOTE_URL="$(git -C "${REPO_ROOT}" remote get-url "${REMOTE}")"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

if git -C "${REPO_ROOT}" ls-remote --exit-code --heads "${REMOTE}" "${PUBLISH_BRANCH}" >/dev/null 2>&1; then
  git clone --depth 1 --branch "${PUBLISH_BRANCH}" "${REMOTE_URL}" "${TMP_DIR}" >/dev/null 2>&1
else
  git init "${TMP_DIR}" >/dev/null
  git -C "${TMP_DIR}" checkout -b "${PUBLISH_BRANCH}" >/dev/null
  git -C "${TMP_DIR}" remote add origin "${REMOTE_URL}"
fi

mkdir -p "${TMP_DIR}/${ROOT}"
rsync -a --delete "${SITE_DIR}/" "${TMP_DIR}/${ROOT}/"
touch "${TMP_DIR}/.nojekyll"

if [[ "${ROOT}" == "latest" || ! -f "${TMP_DIR}/index.html" ]]; then
  cat > "${TMP_DIR}/index.html" <<'EOF'
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta http-equiv="refresh" content="0; url=latest/" />
    <meta name="robots" content="noindex" />
    <title>Cobble Docs</title>
  </head>
  <body>
    <p>Redirecting to <a href="latest/">latest docs</a>…</p>
  </body>
</html>
EOF
fi

git -C "${TMP_DIR}" add -A
if git -C "${TMP_DIR}" diff --cached --quiet; then
  echo "No docs changes to publish for root '${ROOT}'."
  exit 0
fi

git -C "${TMP_DIR}" config user.name "${GIT_AUTHOR_NAME:-github-actions[bot]}"
git -C "${TMP_DIR}" config user.email "${GIT_AUTHOR_EMAIL:-41898282+github-actions[bot]@users.noreply.github.com}"
git -C "${TMP_DIR}" commit -m "docs: publish ${ROOT}" >/dev/null
git -C "${TMP_DIR}" push origin "HEAD:${PUBLISH_BRANCH}" >/dev/null

echo "Published docs root '${ROOT}' to branch '${PUBLISH_BRANCH}'."
