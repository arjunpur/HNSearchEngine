name := "sparkhackernews"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % "2.11.8",
  "org.scala-lang" % "scala-reflect" % "2.11.8",
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  // https://mvnrepository.com/artifact/org.scalamock/scalamock-scalatest-support_2.11
  "org.mockito" % "mockito-core" % "1.10.19",
  "com.typesafe.akka" %% "akka-actor" % "2.4.14",
  "io.spray" % "spray-client_2.11" % "1.3.3",
  "io.spray" % "spray-json_2.11" % "1.3.2",
  "com.github.melrief" %% "purecsv" % "0.0.6",
  "org.rogach" %% "scallop" % "2.0.6"
)

scalacOptions := Seq("-feature", "-language:implicitConversions")
