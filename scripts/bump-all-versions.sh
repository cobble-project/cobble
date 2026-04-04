#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: bump-all-versions.sh <new-version> [options]

Bump repository package versions in one shot:
  - Rust crates: all Cargo.toml [package].version fields
  - Java binding: cobble-java/java/pom.xml project version

Options:
  --java-snapshot     Set Java project version to <new-version>-SNAPSHOT
  --java-release      Set Java project version to <new-version>
  --no-java           Skip Java pom.xml update
  -h, --help          Show this help
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 1 ]]; then
  usage
  exit 2
fi

NEW_VERSION="$1"
shift

JAVA_MODE="auto"
UPDATE_JAVA=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --java-snapshot)
      JAVA_MODE="snapshot"
      shift
      ;;
    --java-release)
      JAVA_MODE="release"
      shift
      ;;
    --no-java)
      UPDATE_JAVA=0
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

if [[ ! "${NEW_VERSION}" =~ ^[0-9]+(\.[0-9]+){1,2}([.-][0-9A-Za-z._-]+)?$ ]]; then
  echo "Invalid version format: ${NEW_VERSION}" >&2
  exit 2
fi

REPO_ROOT="$(git rev-parse --show-toplevel)"

UPDATED_CARGO=0
while IFS= read -r file; do
  if ! grep -qE "^\[package\]" "${file}"; then
    continue
  fi
  before_hash="$(cksum "${file}" | awk '{print $1":"$2}')"
  perl -0777 -i -pe 's{(\[package\][^\[]*?\nversion\s*=\s*")[^"]+(")}{$1'"${NEW_VERSION}"'$2}s' "${file}"
  after_hash="$(cksum "${file}" | awk '{print $1":"$2}')"
  if [[ "${before_hash}" != "${after_hash}" ]]; then
    UPDATED_CARGO=$((UPDATED_CARGO + 1))
  fi
done < <(find "${REPO_ROOT}" -name Cargo.toml -type f | sort)

if [[ "${UPDATE_JAVA}" -eq 1 ]]; then
  POM_FILE="${REPO_ROOT}/cobble-java/java/pom.xml"
  if [[ -f "${POM_FILE}" ]]; then
    CURRENT_JAVA_VERSION="$(perl -0777 -ne 'if (m{<artifactId>\s*cobble\s*</artifactId>\s*<version>\s*([^<]+)\s*</version>}s) { print $1; }' "${POM_FILE}")"
    JAVA_VERSION="${NEW_VERSION}"
    case "${JAVA_MODE}" in
      snapshot)
        JAVA_VERSION="${NEW_VERSION}-SNAPSHOT"
        ;;
      release)
        JAVA_VERSION="${NEW_VERSION}"
        ;;
      auto)
        if [[ "${CURRENT_JAVA_VERSION}" == *"-SNAPSHOT" ]]; then
          JAVA_VERSION="${NEW_VERSION}-SNAPSHOT"
        fi
        ;;
    esac
    perl -0777 -i -pe 's{(<artifactId>\s*cobble\s*</artifactId>\s*<version>\s*)[^<]+(\s*</version>)}{$1'"${JAVA_VERSION}"'$2}s' "${POM_FILE}"
    echo "Updated Java project version: ${CURRENT_JAVA_VERSION} -> ${JAVA_VERSION}"
  else
    echo "Java pom.xml not found, skipped."
  fi
fi

echo "Updated Rust package versions to ${NEW_VERSION} in ${UPDATED_CARGO} Cargo.toml files."
echo "Done."
