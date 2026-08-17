#!/usr/bin/env bash
#
# DEVOPS-6131 - run a build command with sbt resolving Maven Central through the virtana-zing
# Artifact Registry proxy instead of hitting repo1.maven.org directly (jenkins-eng's shared egress
# IP gets 429'd by Central's Cloudflare front-end, which breaks sbt's launcher/dependency/plugin
# downloads). This is the sbt analog of the mirrorMavenCentral shared step - sbt doesn't read
# Maven's settings.xml, so we render sbt's own config from the templates next to this script.
#
# Usage (from a pipeline, gcr_push_key bound as AR_KEY_FILE):
#     withCredentials([file(credentialsId: 'gcr_push_key', variable: 'AR_KEY_FILE')]) {
#         sh 'ci/sbt-mirror/with-mirror.sh make -f ci/Makefile build'
#     }
#
# Design notes addressing the two things we care about:
#   * The AR OAuth token stays IN-PROCESS (exported to the child build only). Nothing writes it to a
#     file that gets passed around; the one transient file that must hold it (coursier's boot creds)
#     is removed by the EXIT trap below, pass or fail - no separate pipeline cleanup step.
#   * gcloud auth runs in a THROWAWAY CLOUDSDK_CONFIG dir, so the agent's own gcloud account is never
#     activated/replaced (agents are reused across builds). This is NOT `gcloud auth login`.
#
# AR_KEY_FILE must point at a GCP service-account key with artifactregistry.reader on
# zing-registry-188222. sbt runs on the agent, so no container mount is needed.

set -eu
: "${AR_KEY_FILE:?with-mirror.sh: AR_KEY_FILE must be set (bind gcr_push_key as a file credential)}"
[ "$#" -ge 1 ] || { echo "with-mirror.sh: usage: with-mirror.sh <build command...>" >&2; exit 2; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MIRROR="https://us-maven.pkg.dev/zing-registry-188222/virtana-zing"
SBT_VERSION="$(awk -F= '/sbt.version/{gsub(/ /,"",$2);print $2}' "${REPO_ROOT}/project/build.properties")"

COURSIER_CREDS="${HOME}/.config/coursier/credentials.properties"
SBT_REPOS="${REPO_ROOT}/.sbt-mirror-repositories"

# Remove every transient auth/config file on exit - the token never outlives this build.
cleanup() {
    rm -f "${COURSIER_CREDS}" "${SBT_REPOS}" "${REPO_ROOT}/.sbtopts" \
          "${REPO_ROOT}/credentials.sbt" "${REPO_ROOT}/project/credentials.sbt"
}
trap cleanup EXIT

# Mint a short-lived AR token in a throwaway gcloud config dir (agent's own account untouched).
# xtrace guarded off so the token is never echoed even if the caller runs us under `bash -x`.
{ set +x; } 2>/dev/null
AR_TOKEN="$(
    CLOUDSDK_CONFIG="$(mktemp -d)"; export CLOUDSDK_CONFIG
    gcloud auth activate-service-account --key-file="${AR_KEY_FILE}" --quiet 1>&2
    gcloud auth print-access-token
    rm -rf "${CLOUDSDK_CONFIG}"
)"
export AR_TOKEN

# repositories: workspace-local (so we don't clobber a shared ~/.sbt/repositories on the agent).
cp "${SCRIPT_DIR}/repositories" "${SBT_REPOS}"

# coursier boot creds (token populated from template) - for the sbt LAUNCHER's shaded coursier.
mkdir -p "$(dirname "${COURSIER_CREDS}")"
while IFS= read -r line || [ -n "${line}" ]; do
    printf '%s\n' "${line//'${AR_TOKEN}'/${AR_TOKEN}}"
done < "${SCRIPT_DIR}/credentials.properties.tmpl" > "${COURSIER_CREDS}"
chmod 600 "${COURSIER_CREDS}"

# lm-coursier + Ivy creds for deps + plugins - read $AR_TOKEN from the env (no secret on disk).
cp "${SCRIPT_DIR}/credentials.sbt" "${REPO_ROOT}/credentials.sbt"
cp "${SCRIPT_DIR}/credentials.sbt" "${REPO_ROOT}/project/credentials.sbt"

# force sbt to use ONLY the mirror repositories (no fallback to Central).
cat > "${REPO_ROOT}/.sbtopts" <<EOF
-Dsbt.repository.config=${SBT_REPOS}
-Dsbt.override.build.repos=true
EOF

# pre-fetch the sbt launch jar through the mirror (the bare curl in build/sbt-launch-lib.bash
# can't send an auth header itself).
mkdir -p "${REPO_ROOT}/build"
curl --fail --location --silent -H "Authorization: Bearer ${AR_TOKEN}" \
     "${MIRROR}/org/scala-sbt/sbt-launch/${SBT_VERSION}/sbt-launch-${SBT_VERSION}.jar" \
     -o "${REPO_ROOT}/build/sbt-launch-${SBT_VERSION}.jar"

echo "with-mirror.sh: sbt -> ${MIRROR} (sbt ${SBT_VERSION}); running: $*"
"$@"
