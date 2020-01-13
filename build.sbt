name := "magnolia-cdk-glue-firehose"

version := "0.1"

scalaVersion := "2.13.1"

mainClass := Some("demo.Main")

val CdkVersion = "1.20.0"

libraryDependencies ++= Seq(
  "com.propensive" %% "magnolia" % "0.12.6",
  "software.amazon.awscdk" % "glue" % CdkVersion,
  "software.amazon.awscdk" % "kinesisfirehose" % CdkVersion,
  "software.amazon.awscdk" % "logs" % CdkVersion
)
