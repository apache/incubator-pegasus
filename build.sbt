version := "1.0.0-SNAPSHOT"

organization := "com.xiaomi"

name := "pegasus-scala-client"

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "20.0",
  "com.xiaomi.infra" % "pegasus-client" % "1.7.2-thrift-0.11.0-inlined",
   "org.scalatest" %% "scalatest" % "3.0.3" % Test
)
