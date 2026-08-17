// DEVOPS-6131 (generated copy - do not edit here; source: ci/sbt-mirror/credentials.sbt).
// sbt's lm-coursier only authenticates via the native `credentials` setting (not the coursier
// properties file / COURSIER_CREDENTIALS env). ci/sbt-mirror/setup.sh drops this at both the build
// root and project/ (meta build) so library AND plugin resolution can reach the virtana-zing mirror.
// The token comes from the AR_TOKEN env var (exported by sourcing .ar-token.env before the build),
// so no secret lives in this file; with AR_TOKEN unset (e.g. a local dev build not using the
// mirror) it adds no credentials.
credentials ++= sys.env.get("AR_TOKEN").filter(_.nonEmpty).map { token =>
  Credentials("Artifact Registry", "us-maven.pkg.dev", "oauth2accesstoken", token)
}.toSeq
