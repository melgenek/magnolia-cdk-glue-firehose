name := "magnolia-cdk-glue-firehose"

version := "0.1"

inThisBuild(
  scalaVersion := "2.13.6"
)

val CdkVersion = "1.127.0"
val CirceVersion = "0.14.1"

lazy val common = project
  .in(file("common"))

lazy val client = project
  .in(file("client"))
  .settings(
    libraryDependencies ++= List(
      "software.amazon.awssdk" % "kinesis" % "2.17.58",
      "io.circe" %% "circe-core" % CirceVersion,
      "io.circe" %% "circe-generic" % CirceVersion,
      "io.circe" %% "circe-parser" % CirceVersion
    )
  )
  .dependsOn(common)

lazy val infra = project
  .in(file("infra"))
  .settings(
    mainClass := Some("demo.Main"),
    libraryDependencies ++= List(
      "com.softwaremill.magnolia1_2" %% "magnolia" % "1.0.0-M7",
      "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided,
      "software.amazon.awscdk" % "glue" % CdkVersion,
      "software.amazon.awscdk" % "kinesisfirehose" % CdkVersion,
      "software.amazon.awscdk" % "kinesis" % CdkVersion,
      "software.amazon.awscdk" % "logs" % CdkVersion
    )
  )
  .dependsOn(common)
