#!/usr/bin/env bash
#
# DEVOPS-6131 - configure sbt to resolve Maven Central through the virtana-zing Artifact Registry
# proxy instead of hitting repo1.maven.org directly (jenkins-eng's shared egress IP gets 429'd by
# Central's Cloudflare front-end, which breaks sbt's launcher + dependency/plugin downloads).
#
# This is the sbt analog of the mirrorMavenCentral shared step: sbt doesn't read Maven's
# settings.xml, so we render sbt's own config from the templates next to this script and populate
# the short-lived credential on the fly.
#
# Usage (from a pipeline, credential bound as AR_KEY_FILE):
#     withCredentials([file(credentialsId: 'gcr_push_key', variable: 'AR_KEY_FILE')]) {
#         sh 'ci/sbt-mirror/setup.sh && make -f ci/Makefile build'
#     }
#
# AR_KEY_FILE must point at a GCP service-account key with artifactregistry.reader on
# zing-registry-188222. sbt here runs directly on the Jenkins agent, so config is written to the
# agent HOME - no container mount needed. (If a build ran sbt inside a container, mount $HOME/.sbt
# and $HOME/.config/coursier into it.)
#
# Cleanup of the token-bearing files ($HOME/.sbt/.ar-token, coursier credentials.properties) is the
# caller's responsibility - do it in the pipeline's finally block.

set -eu

: "${AR_KEY_FILE:?ci/sbt-mirror/setup.sh: AR_KEY_FILE (path to a GCP SA key) must be set}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MIRROR="https://us-maven.pkg.dev/zing-registry-188222/virtana-zing"
SBT_VERSION="$(awk -F= '/sbt.version/{gsub(/ /,"",$2);print $2}' "${REPO_ROOT}/project/build.properties")"

# Mint a short-lived AR token in a throwaway gcloud config dir so the agent's own active gcloud
# account is left untouched (agents are reused across builds). xtrace off so it never hits the log.
{ set +x; } 2>/dev/null
AR_TOKEN="$(
    CLOUDSDK_CONFIG="$(mktemp -d)"; export CLOUDSDK_CONFIG
    gcloud auth activate-service-account --key-file="${AR_KEY_FILE}" --quiet 1>&2
    gcloud auth print-access-token
    rm -rf "${CLOUDSDK_CONFIG}"
)"

# 1) repositories: maven-central -> mirror (static template, no secret).
mkdir -p "${HOME}/.sbt" "${HOME}/.config/coursier"
cp "${SCRIPT_DIR}/repositories" "${HOME}/.sbt/repositories"

# 2) credentials for the mirror host:
#    a) coursier properties (token populated from template) -> the sbt LAUNCHER's shaded coursier (boot);
#    b) token file + credentials.sbt at build root AND project/ (meta) -> lm-coursier for deps + plugins.
while IFS= read -r line || [ -n "${line}" ]; do
    printf '%s\n' "${line//'${AR_TOKEN}'/${AR_TOKEN}}"
done < "${SCRIPT_DIR}/credentials.properties.tmpl" > "${HOME}/.config/coursier/credentials.properties"

printf '%s' "${AR_TOKEN}" > "${HOME}/.sbt/.ar-token"
chmod 600 "${HOME}/.sbt/.ar-token" "${HOME}/.config/coursier/credentials.properties"
cp "${SCRIPT_DIR}/credentials.sbt" "${REPO_ROOT}/credentials.sbt"
cp "${SCRIPT_DIR}/credentials.sbt" "${REPO_ROOT}/project/credentials.sbt"

# 3) pre-fetch the sbt launch jar through the mirror (build/sbt-launch-lib.bash's bare curl can't auth).
mkdir -p "${REPO_ROOT}/build"
curl --fail --location --silent -H "Authorization: Bearer ${AR_TOKEN}" \
     "${MIRROR}/org/scala-sbt/sbt-launch/${SBT_VERSION}/sbt-launch-${SBT_VERSION}.jar" \
     -o "${REPO_ROOT}/build/sbt-launch-${SBT_VERSION}.jar"

# 4) force sbt to use ONLY the mirror repositories (no fallback to Central). build/sbt reads .sbtopts
#    from the repo root at launch, so no env needs to survive into the separate `make` process.
cat > "${REPO_ROOT}/.sbtopts" <<EOF
-Dsbt.repository.config=${HOME}/.sbt/repositories
-Dsbt.override.build.repos=true
EOF

echo "ci/sbt-mirror/setup.sh: sbt configured to resolve Central via ${MIRROR} (sbt ${SBT_VERSION})"
