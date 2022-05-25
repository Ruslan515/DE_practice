ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.11.8"

val sparkVersion = "2.3.2"

lazy val root = (project in file("."))
    .settings(
        name := "datamarts-rkhalikov"
    )

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion  % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided",
    "org.postgresql" % "postgresql" % "42.3.3"
)

assembly / artifact := {
    val art = (assembly / artifact).value
    art.withClassifier(Some("assembly"))
}

addArtifact(assembly / artifact, assembly)

assemblyMergeStrategy in assembly := {
    case path if path.contains("META-INF/services") => MergeStrategy.concat
    case PathList("META-INF", _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
}