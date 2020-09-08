addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0" excludeAll(
  ExclusionRule(organization = "com.danieltrinh")))
libraryDependencies += "org.scalariform" %% "scalariform" % "0.2.9"

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.2")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.2-1")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.3.0")
