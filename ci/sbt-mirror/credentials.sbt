// DEVOPS-6131 (generated copy - do not edit here; source: ci/sbt-mirror/credentials.sbt).
// sbt's lm-coursier only authenticates via the native `credentials` setting (not the coursier
// properties file / COURSIER_CREDENTIALS env). ci/sbt-mirror/setup.sh drops this at both the build
// root and project/ (meta build) so library AND plugin resolution can reach the virtana-zing mirror.
// The token comes from the AR_TOKEN env var (exported by sourcing .ar-token.env before the build),
// so no secret lives in this file; with AR_TOKEN unset (local dev, mirror not used) the password is
// empty and this credential is simply never matched/used.
//
// The realm MUST be "https://us-maven.pkg.dev" (exactly what AR sends in its WWW-Authenticate
// header). coursier matches credentials by host alone, but sbt's Ivy code path matches by realm too;
// an empty realm makes Ivy log "Unable to find credentials for [https://us-maven.pkg.dev @ ...]"
// during makePom/metadata even though the build still succeeds via coursier. Matching the realm
// silences those.
credentials += Credentials("https://us-maven.pkg.dev", "us-maven.pkg.dev", "oauth2accesstoken", sys.env.getOrElse("AR_TOKEN", ""))
