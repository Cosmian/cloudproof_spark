name := "Simple Project"

version := "1.0"

scalaVersion := "2.12.17"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"
libraryDependencies += "com.databricks" %% "spark-xml" % "0.16.0"
libraryDependencies += "com.cosmian" % "cloudproof_java" % "4.0.1"