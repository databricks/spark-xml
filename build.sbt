name := "spark-xml"

version := "0.5.0"

organization := "com.databricks"

scalaVersion := "2.11.12"

spName := "databricks/spark-xml"

crossScalaVersions := Seq("2.11.12", "2.12.8")

// Necessary for JUnit tests to be found?
crossPaths := false

scalacOptions := Seq("-unchecked", "-deprecation")

sparkVersion := sys.props.get("spark.testVersion").getOrElse("2.4.0")

sparkComponents := Seq("core", "sql")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.25" % Provided,
  "org.scalatest" %% "scalatest" % "3.0.3" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.apache.spark" %% "spark-core" % sparkVersion.value % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % Test,
  "org.scala-lang" % "scala-library" % scalaVersion.value % Provided
)

// This is necessary because of how we explicitly specify Spark dependencies
// for tests rather than using the sbt-spark-package plugin to provide them.
spIgnoreProvided := true

publishMavenStyle := true

spAppendScalaVersion := true

spIncludeMaven := true

pomExtra :=
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
  </developers>

parallelExecution in Test := false

// Skip tests during assembly
test in assembly := {}

// Prints JUnit tests in output
testOptions in Test := Seq(Tests.Argument(TestFrameworks.JUnit, "-v"))
