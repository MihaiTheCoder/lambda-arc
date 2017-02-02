import sbt._
import Keys._
import sbtassembly.Plugin._
// -----------------------------
// project definition
// -----------------------------

name := "lambda-arc"

// -----------------------------
// Add your stuff here
// -----------------------------

// -----------------------------
// resolvers (source repositories)
// -----------------------------

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)
lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "com.qubiz.bigdata",
  scalaVersion := "2.11.8"
)

lazy val root =
  project.in( file(".") )
  .settings(commonSettings: _*)
  .settings(assemblySettings: _*)
  .aggregate(sparkLambda)


lazy val sparkLambda = (project in file("spark-lambda"))
  .settings(commonSettings: _*)
  .settings(assemblySettings: _*)


// -----------------------------
// Publishing rules
// -----------------------------
resolvers += DefaultMavenRepository

libraryDependencies += "com.typesafe" % "config" % "1.3.0"





