name := "spark-xml"

version := "0.4.2"

organization := "com.databricks"

scalaVersion := "2.11.7"

spName := "databricks/spark-xml"

crossScalaVersions := Seq("2.10.5", "2.11.7")

sparkVersion := "2.0.0"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

val testHadoopVersion = settingKey[String]("The version of Hadoop to test against.")

testHadoopVersion := sys.props.getOrElse("hadoop.testVersion", "2.2.0")

sparkComponents := Seq("core", "sql")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.5" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.novocode" % "junit-interface" % "0.9" % "test"
)

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % testHadoopVersion.value % "test",
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" force() exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test" force() exclude("org.apache.hadoop", "hadoop-client"),
  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile"
)

// This is necessary because of how we explicitly specify Spark dependencies
// for tests rather than using the sbt-spark-package plugin to provide them.
spIgnoreProvided := true

publishMavenStyle := true

spAppendScalaVersion := true

spIncludeMaven := true

pomExtra := (
  <url>https://github.com/databricks/spark-xml</url>
  <licenses>
    <license>
      <name>Apache License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:databricks/spark-xml.git</url>
    <connection>scm:git:git@github.com:databricks/spark-xml.git</connection>
  </scm>
  <developers>
    <developer>
      <id>hyukjinkwon</id>
      <name>Hyukjin Kwon</name>
      <url>https://www.facebook.com/hyukjin.kwon.96</url>
    </developer>
  </developers>)

parallelExecution in Test := false

// Skip tests during assembly
test in assembly := {}

ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else true
}
