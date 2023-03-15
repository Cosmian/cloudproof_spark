name := "CloudproofSpark"

version := "1.0"

scalaVersion := "2.12.17"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"
libraryDependencies += "com.databricks" %% "spark-xml" % "0.16.0"
libraryDependencies += "com.cosmian" % "cloudproof_java" % "4.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.11" % "test"

// Fixes a problem with the Jackson version used by downgrading it
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.3" % "test",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.3" % "test"
)

// Test options
Test / testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a", "-Dtest.time=true")
Test / logBuffered := false
Test / parallelExecution := false
