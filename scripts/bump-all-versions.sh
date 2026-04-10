#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: bump-all-versions.sh <new-version> [options]

Bump repository package versions in one shot:
  - Rust crates:
      * [workspace.package].version
      * [package].version
      * local path dependency inline-table versions (path + version)
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
ROOT_CARGO_TOML="${REPO_ROOT}/Cargo.toml"

OLD_VERSION="$(perl -0777 -ne 'if (m{\[workspace\.package\][^\[]*?\nversion\s*=\s*"([^"]+)"}s) { print $1; }' "${ROOT_CARGO_TOML}")"
if [[ -z "${OLD_VERSION}" ]]; then
  echo "Failed to read [workspace.package].version from ${ROOT_CARGO_TOML}" >&2
  exit 1
fi

UPDATED_CARGO=0
while IFS= read -r file; do
  before_hash="$(cksum "${file}" | awk '{print $1":"$2}')"
  NEW_VERSION_ENV="${NEW_VERSION}" perl -0777 -i -pe '
    BEGIN { $v = $ENV{"NEW_VERSION_ENV"}; }
    s#(\[workspace\.package\][^\[]*?\nversion\s*=\s*")[^"]+(")#$1$v$2#s;
    s#(\[package\][^\[]*?\nversion\s*=\s*")[^"]+(")#$1$v$2#s;
    s#(\bpath\s*=\s*"[^"]+"[^\n]*\bversion\s*=\s*")[^"]+(")#$1$v$2#g;
  ' "${file}"
  after_hash="$(cksum "${file}" | awk '{print $1":"$2}')"
  if [[ "${before_hash}" != "${after_hash}" ]]; then
    UPDATED_CARGO=$((UPDATED_CARGO + 1))
  fi
done < <(find "${REPO_ROOT}" -name Cargo.toml -type f | sort)

UPDATED_LOCK=0
if [[ -f "${REPO_ROOT}/Cargo.lock" && "${OLD_VERSION}" != "${NEW_VERSION}" ]]; then
  workspace_package_names="$(
    cd "${REPO_ROOT}" && cargo metadata --format-version 1 --no-deps | python3 -c '
import json
import sys

data = json.load(sys.stdin)
workspace_members = set(data.get("workspace_members", []))
for package in data.get("packages", []):
    if package.get("id") in workspace_members:
        print(package.get("name"))
'
  )"

  if [[ -n "${workspace_package_names}" ]]; then
    before_hash="$(cksum "${REPO_ROOT}/Cargo.lock" | awk "{print \$1\":\"\$2}")"
    while IFS= read -r package_name; do
      [[ -z "${package_name}" ]] && continue
      OLD_VERSION_ENV="${OLD_VERSION}" NEW_VERSION_ENV="${NEW_VERSION}" PACKAGE_NAME_ENV="${package_name}" perl -0777 -i -pe '
        BEGIN {
          $old = $ENV{"OLD_VERSION_ENV"};
          $new = $ENV{"NEW_VERSION_ENV"};
          $pkg = $ENV{"PACKAGE_NAME_ENV"};
        }
        s#(\[\[package\]\]\nname = "\Q$pkg\E"\nversion = ")\Q$old\E(")#$1$new$2#g;
      ' "${REPO_ROOT}/Cargo.lock"
    done <<< "${workspace_package_names}"
    after_hash="$(cksum "${REPO_ROOT}/Cargo.lock" | awk "{print \$1\":\"\$2}")"
    if [[ "${before_hash}" != "${after_hash}" ]]; then
      UPDATED_LOCK=1
    fi
  fi
fi

UPDATED_DOCS=0
if [[ "${OLD_VERSION}" != "${NEW_VERSION}" ]]; then
  while IFS= read -r file; do
    before_hash="$(cksum "${file}" | awk "{print \$1\":\"\$2}")"
    OLD_VERSION_ENV="${OLD_VERSION}" NEW_VERSION_ENV="${NEW_VERSION}" perl -0777 -i -pe '
      BEGIN { $old = $ENV{"OLD_VERSION_ENV"}; $new = $ENV{"NEW_VERSION_ENV"}; }
      s#(\bcobble\s*=\s*")\Q$old\E(")#$1$new$2#g;
      s#(\bcobble\s*=\s*\{[^}\n]*\bversion\s*=\s*")\Q$old\E(")#$1$new$2#g;
      s#(<artifactId>\s*cobble\s*</artifactId>\s*<version>\s*)\Q$old\E(\s*</version>)#$1$new$2#gs;
    ' "${file}"
    after_hash="$(cksum "${file}" | awk "{print \$1\":\"\$2}")"
    if [[ "${before_hash}" != "${after_hash}" ]]; then
      UPDATED_DOCS=$((UPDATED_DOCS + 1))
    fi
  done < <(find "${REPO_ROOT}" \( -path "${REPO_ROOT}/docs/*" -o -path "${REPO_ROOT}/README.md" \) -type f -name "*.md" | sort)
fi

if [[ "${UPDATE_JAVA}" -eq 1 ]]; then
  POM_FILE="${REPO_ROOT}/cobble-java/java/pom.xml"
  if [[ -f "${POM_FILE}" ]]; then
    CURRENT_JAVA_VERSION="$(perl -0777 -ne 'if (m{<project\b.*?<version>\s*([^<]+)\s*</version>}s) { print $1; }' "${POM_FILE}")"
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
    JAVA_VERSION_ENV="${JAVA_VERSION}" perl -0777 -i -pe '
      BEGIN { $v = $ENV{"JAVA_VERSION_ENV"}; }
      s{(<project\b.*?<version>\s*)[^<]+(\s*</version>)}{$1$v$2}s
    ' "${POM_FILE}"
    echo "Updated Java project version: ${CURRENT_JAVA_VERSION} -> ${JAVA_VERSION}"
  else
    echo "Java pom.xml not found, skipped."
  fi
fi

echo "Updated Rust package versions to ${NEW_VERSION} in ${UPDATED_CARGO} Cargo.toml files."
echo "Updated Cargo.lock workspace package versions: ${UPDATED_LOCK}."
echo "Updated Markdown version references: ${UPDATED_DOCS} file(s)."
echo "Done."
