name := "sparkhackernews"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % "2.11.8",
  "org.scala-lang" % "scala-reflect" % "2.11.8",
  // https://mvnrepository.com/artifact/org.scalamock/scalamock-scalatest-support_2.11
  "org.mockito" % "mockito-core" % "1.10.19",
  "com.typesafe.akka" %% "akka-actor" % "2.4.14",
  "io.spray" % "spray-client_2.11" % "1.3.3",
  "io.spray" % "spray-json_2.11" % "1.3.2",
  "org.rogach" %% "scallop" % "2.0.6",
  "ch.qos.logback" % "logback-classic" % "1.2.2",
  "org.slf4j" % "slf4j-log4j12" % "1.2",
  "org.slf4j" % "slf4j-simple" % "1.7.21",
  "org.apache.logging.log4j" % "log4j-api" % "2.6.2",
  "org.apache.logging.log4j" % "log4j-to-slf4j" % "2.6.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "com.amazonaws" % "aws-java-sdk" % "1.11.86",
  "org.elasticsearch.client" % "transport" % "5.2.2"
)

scalacOptions := Seq("-feature", "-language:implicitConversions")

