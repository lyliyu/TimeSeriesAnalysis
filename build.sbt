
import AssemblyKeys._

name := "TimeSeriesAnalysis"

version := "1.0"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1" % "provided",
  "junit" % "junit" % "4.12" % "test",
  "org.apache.spark" %% "spark-mllib" % "1.3.1" % "provided",
  "com.novocode" % "junit-interface" % "0.11" % "test->default",
  "scalation" % "scalation_2.11" % "1.1",
  "org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.3.1" excludeAll(ExclusionRule(organization = "javax.servlet")),
  "org.mongodb" % "mongo-java-driver" % "2.12.4"
)

// Load Assembly Settings

assemblySettings

// Assembly App

// Do not run test when when assembling the Learning Engine fat Jar
test in assembly := {}

jarName in assembly := "timeseriesanalysis.jar"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("org", "apache", "commons", xs @ _*)  => MergeStrategy.first
  case PathList("org", "apache", "hadoop", xs @ _*)  => MergeStrategy.last
  case x => old(x)
}
}