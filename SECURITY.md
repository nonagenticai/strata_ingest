# Security policy

## Supported versions

strata_ingest is in active development. Security fixes land on the latest release line
only; older tags are not patched. If you are running an older version, the fix is to
upgrade.

| Version | Supported |
|---------|-----------|
| latest `latest` | yes |
| older tags | no |

## Reporting a vulnerability

Please report privately, through GitHub's advisory flow:

**https://github.com/nonagenticai/strata_ingest/security/advisories/new**

Do not open a public issue, and do not post details in a discussion. A public issue
turns a fixable bug into a disclosure.

Please include:

- A description of the issue and its impact.
- Steps to reproduce, or a proof of concept.
- Affected version(s) or commit hashes.
- Any suggested mitigation.

## What to expect

| Stage | Target |
|-------|--------|
| Acknowledgement of your report | 5 business days |
| Initial triage and severity assessment | 10 business days |
| Fix for high-severity issues | 30 days |
| Coordinated public disclosure | 90 days, or sooner by agreement |

We will keep you informed at each stage and credit you in the advisory unless you ask
us not to.

## Scope

In scope: the code in this repository, and the ways it can be made to misbehave —
authentication and authorization bypasses, injection, path traversal, unsafe
deserialization, secret leakage, and denial of service.

Out of scope: vulnerabilities in third-party dependencies (report those upstream, and we
will pick up the fix through a dependency update), issues that require a compromised
host or an attacker who already has the deployment's credentials, and findings from
automated scanners without a demonstrated impact.

## Deployment-specific responsibility

This repository is source code. Whoever operates a deployment of it is responsible for
that deployment's configuration, its network exposure, and its credentials.
