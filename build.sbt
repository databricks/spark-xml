import com.typesafe.tools.mima.core.MissingClassProblem

name := "spark-xml"

version := "0.17.0"

organization := "com.databricks"

scalaVersion := "2.12.15"

crossScalaVersions := Seq("2.12.15", "2.13.8")

scalacOptions := Seq("-unchecked", "-deprecation")

val sparkVersion = sys.props.get("spark.testVersion").getOrElse("3.3.1")

// To avoid packaging it, it's Provided below
autoScalaLibrary := false

libraryDependencies ++= Seq(
  "commons-io" % "commons-io" % "2.11.0",
  "org.glassfish.jaxb" % "txw2" % "3.0.2",
  "org.apache.ws.xmlschema" % "xmlschema-core" % "2.3.0",
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.9.0",
  "org.slf4j" % "slf4j-api" % "1.7.36" % Provided,
  "org.scalatest" %% "scalatest" % "3.2.14" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.scala-lang" % "scala-library" % scalaVersion.value % Provided
)

publishMavenStyle := true

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
    </developer>
    <developer>
      <id>srowen</id>
      <name>Sean Owen</name>
    </developer>
  </developers>

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  sys.env.getOrElse("USERNAME", ""),
  sys.env.getOrElse("PASSWORD", ""))

resolvers +=
  "GCS Maven Central mirror" at "https://maven-central.storage-download.googleapis.com/maven2/"

Test / parallelExecution := false

// Skip tests during assembly
assembly / test := {}

fork := true

// Prints JUnit tests in output
Test / testOptions := Seq(Tests.Argument(TestFrameworks.JUnit, "-v"))

mimaPreviousArtifacts := Set("com.databricks" %% "spark-xml" % "0.16.0")

mimaBinaryIssueFilters ++= {
  import com.typesafe.tools.mima.core.{DirectMissingMethodProblem,
    IncompatibleMethTypeProblem, IncompatibleResultTypeProblem}
  import com.typesafe.tools.mima.core.ProblemFilters.exclude
  Seq(
    exclude[IncompatibleMethTypeProblem]("com.databricks.spark.xml.XmlRelation.apply"),
    exclude[IncompatibleMethTypeProblem]("com.databricks.spark.xml.XmlRelation.copy"),
    exclude[IncompatibleMethTypeProblem]("com.databricks.spark.xml.XmlRelation.this"),
    exclude[IncompatibleMethTypeProblem]("com.databricks.spark.xml.XmlRelation.apply"),
    exclude[IncompatibleResultTypeProblem]("com.databricks.spark.xml.XmlRelation.copy$default$2"),
    exclude[DirectMissingMethodProblem]("com.databricks.spark.xml.XmlRelation.location"),

  )
}
