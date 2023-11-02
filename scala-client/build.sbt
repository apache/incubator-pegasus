/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

version := "2.5.0-SNAPSHOT"

organization := "org.apache"

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
  "org.apache.pegasus" % "pegasus-client" % "2.5.0-SNAPSHOT",
  "org.scalatest" %% "scalatest" % "3.0.3" % Test
)
