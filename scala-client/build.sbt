version := "2.2.0-3-release"

organization := "com.xiaomi.infra"

name := "pegasus-scala-client"

scalaVersion := "2.12.7"

crossScalaVersions := Seq("2.11.7", "2.12.7")

publishMavenStyle := true

scalafmtOnCompile := true

//custom repository
resolvers ++= Seq(
  //"Remote Maven Repository" at "http://your-url/",
  "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
)

//custom publish url
publishTo := {
  //releases
  val releases = "https://"
  //snapshots
  val snapshots = "https://"
  if (isSnapshot.value)
    Some(
      "Artifactory Realm" at snapshots + ";build.timestamp=" + new java.util.Date().getTime)
  else Some("Artifactory Realm" at releases)
}

credentials += Credentials(
  new File((Path.userHome / ".sbt" / ".credentials").toString()))

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "21.0",
  "com.xiaomi.infra" % "pegasus-client" % "2.2.0",
  "org.scalatest" %% "scalatest" % "3.0.3" % Test
)
