import Dependencies._

scalaVersion     := "2.13.3"
version          := "0.1.0-SNAPSHOT"
organization     := "com.example"
organizationName := "example"

resolvers += "io.confluent" at "https://packages.confluent.io/maven/"

lazy val root = (project in file("."))
  .settings(
    name := "info.tri_comfort.kafka",
    scalaVersion:= "2.13.3",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      "org.apache.kafka" %% "kafka-streams-scala" % "2.6.0",
      "org.apache.kafka" % "kafka-streams-test-utils" % "2.6.0" % Test,
      "io.circe" %% "circe-core" % "0.13.0",
      "io.circe" %% "circe-generic" % "0.13.0",
      "io.circe" %% "circe-parser" % "0.13.0",
      "org.slf4j" % "slf4j-api" % "1.7.30", 
      "org.slf4j" % "slf4j-log4j12" % "1.7.30",
      "com.goyeau" %% "kafka-streams-circe" % "28232c9",
      "io.confluent" % "kafka-streams-avro-serde" % "6.0.0",
      "io.confluent" % "kafka-avro-serializer" % "6.0.0",
      "org.apache.avro" % "avro" % "1.9.2",
      "org.apache.avro" % "avro-maven-plugin" % "1.9.2"
    )
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
