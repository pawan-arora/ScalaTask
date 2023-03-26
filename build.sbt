val scala3Version = "3.2.1"

lazy val root = project
  .in(file("."))
  .settings(
    name := "interviewpractice",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq("org.scalameta" %% "munit" % "0.7.29" % Test,
      "com.typesafe.akka" %% "akka-stream" % "2.8.0", "com.github.tototoshi" %% "scala-csv" % "1.3.10")
  )
