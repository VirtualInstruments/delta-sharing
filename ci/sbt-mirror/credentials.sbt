// DEVOPS-6131 (generated copy - do not edit here; source: ci/sbt-mirror/credentials.sbt).
// sbt's lm-coursier only authenticates via the native `credentials` setting (not the coursier
// properties file / COURSIER_CREDENTIALS env). ci/sbt-mirror/setup.sh drops this at both the build
// root and project/ (meta build) so library AND plugin resolution can reach the virtana-zing mirror.
// The token is read from ~/.sbt/.ar-token (written by setup.sh) so no secret lives in this file;
// if the token file is absent (e.g. a local dev build not using the mirror), it adds no credentials.
credentials ++= {
  val tokenFile = file(sys.props("user.home")) / ".sbt" / ".ar-token"
  if (tokenFile.exists)
    Seq(Credentials("Artifact Registry", "us-maven.pkg.dev", "oauth2accesstoken", IO.read(tokenFile).trim))
  else
    Seq.empty
}
