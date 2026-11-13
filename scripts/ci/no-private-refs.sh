#!/usr/bin/env bash
# Refuse to publish private infrastructure.
#
# This is the second half of the public-repo rule. `gitleaks` catches credentials; this catches
# the identifiers around them: internal hostnames, tailnet addresses, host names and private
# repository paths. It scans every tracked file, so it also covers docs, fixtures and recorded
# test data — the places a scrub forgets.
#
# Exit 1 lists every violation. Exit 0 prints the allowlisted and warn-only hits.
#
# Allowlist: `.ci/private-refs.allow`, one rule per line:
#     path/glob:pattern  # why this line is safe to publish
# `path/glob` is a shell glob matched against the repo-relative path; `pattern` is an ERE matched
# against the whole `git grep` hit (`path:line:text`). A justification is mandatory in review —
# the guard does not enforce it, humans do.
#
# Run it locally before pushing: `bash scripts/ci/no-private-refs.sh`.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 2

# Hard failures. No \b: BSD/macOS regcomp has no word boundary, and this must behave the same on
# the dev box and on the runner. The two trailing-boundary groups below are hand-rolled stand-ins
# for \b, for that reason.
HARD=(
  # Needs a boundary AFTER the word, not before. Measured on `licenses`: a bare `\.internal`
  # matched the Python function name `main.internal_mint_tenant_bundle_token` on four lines of
  # migrations, i.e. a false positive on the very repos the guard exists to clear. A hostname is
  # always followed by a delimiter (port, path, quote, space, end of line); a snake_case
  # identifier is followed by `_`.
  # Measured again on `agent` 2026-11-13: a Rust test helper `c.internal(…)` on 107 lines. A
  # method call is followed by `(`, a hostname never is — so `(` is excluded too. This is the
  # difference between a guard someone trusts and one they learn to ignore.
  '\.internal([^A-Za-z0-9_(]|$)'
  # Kubernetes service DNS. `\.internal` does not match it, and four public READMEs
  # were published carrying `licenses.licenses.svc.cluster.local:8544` (measured
  # 2026-11-13) — internal topology an outsider can map a deployment with.
  '\.svc\.cluster\.local'
  '100\.(6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\.[0-9]{1,3}\.[0-9]{1,3}'
  # Needs a boundary BEFORE the word. A bare `ts\.net` matched `parts.netloc` in `licenses`'
  # models.py (the `t` of `parts` plus `.netloc`) — plain Python attribute access, not a tailnet
  # MagicDNS name. A real one is preceded by `.` or a quote, never by an identifier character.
  '(^|[^A-Za-z0-9_-])ts\.net'
  'strato-(server|vps)'
  'nonagenticai/gitops-production'
  # The licence-token shape, and the reason this guard exists in addition to a secret
  # scanner. A licence token is 43 base64url characters — below the entropy floor of every
  # scanner the estate runs, so it has propagated silently through 14 repos. A bare
  # `[A-Za-z0-9_-]{43}` is useless here: measured on `agent`'s tree it matches 30 snake_case
  # test-function names. Anchor on a quoted literal on a line that also names a licence or
  # token, which is how one is actually written into code.
  "(licen[cs]e|LICEN[CS]E|token|TOKEN).{0,60}[\"'][A-Za-z0-9_-]{43}[\"']"
  'Authorization: *Bearer [A-Za-z0-9_-]{43}'
)

# Warnings only: RFC1918 literals are usually a test fixture, but they are still an address.
SOFT=(
  '192\.168\.[0-9]{1,3}\.[0-9]{1,3}'
  '(^|[^0-9.])10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}'
)

allowed() {
  # $1 = full hit line; returns 0 when an allowlist rule covers it.
  local hit="$1" file="${1%%:*}" rule glob pat
  [ -f .ci/private-refs.allow ] || return 1
  while IFS= read -r rule || [ -n "$rule" ]; do
    rule="${rule%%#*}"
    rule="$(printf '%s' "$rule" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
    [ -n "$rule" ] || continue
    glob="${rule%%:*}"
    pat="${rule#*:}"
    pat="$(printf '%s' "$pat" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
    [ -n "$glob" ] && [ -n "$pat" ] || continue
    # shellcheck disable=SC2254 # glob match is intended
    case "$file" in
      $glob) printf '%s\n' "$hit" | grep -qE -- "$pat" && return 0 ;;
    esac
  done < .ci/private-refs.allow
  return 1
}

hard_args=()
for p in "${HARD[@]}"; do hard_args+=(-e "$p"); done
soft_args=()
for p in "${SOFT[@]}"; do soft_args+=(-e "$p"); done

# This script and its allowlist necessarily spell the patterns out; everything else is scanned.
SCAN=(-- . ':(exclude)scripts/ci/no-private-refs.sh' ':(exclude).ci/private-refs.allow')

fail=0
allow_count=0
while IFS= read -r hit; do
  [ -n "$hit" ] || continue
  if allowed "$hit"; then
    allow_count=$((allow_count + 1))
    echo "allowed: $hit"
  else
    echo "PRIVATE: $hit"
    fail=1
  fi
done < <(git grep -nI -E "${hard_args[@]}" "${SCAN[@]}" || true)

while IFS= read -r hit; do
  [ -n "$hit" ] || continue
  echo "warn (RFC1918): $hit"
done < <(git grep -nI -E "${soft_args[@]}" "${SCAN[@]}" || true)

echo
if [ "$fail" -ne 0 ]; then
  echo "FAILED: private infrastructure is referenced in tracked files (see PRIVATE lines above)."
  echo "Remove it, or add a reviewed rule to .ci/private-refs.allow with a justification."
  exit 1
fi
echo "OK: no private infrastructure references in $(git ls-files | wc -l | tr -d ' ') tracked files ($allow_count allowlisted)."
