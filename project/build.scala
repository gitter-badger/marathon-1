import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import sbtrelease.ReleasePlugin._

object MarathonBuild extends Build {
  lazy val root = Project(
    id = "marathon",
    base = file("."),
    settings = baseSettings ++ assemblySettings ++ releaseSettings ++ Seq(
      libraryDependencies ++= Dependencies.root
    )
  )

  lazy val baseSettings = Defaults.defaultSettings ++ Seq (
    organization := "mesosphere",
    scalaVersion := "2.10.4",
    scalacOptions in Compile ++= Seq("-encoding", "UTF-8", "-target:jvm-1.6", "-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),
    javacOptions in Compile ++= Seq("-encoding", "UTF-8", "-source", "1.6", "-target", "1.6", "-Xlint:unchecked", "-Xlint:deprecation"),
    resolvers ++= Seq(
      "Mesosphere Public Repo" at "http://downloads.mesosphere.io/maven",
      "Twitter Maven2 Repository" at "http://maven.twttr.com/",
      "Spray Maven Repository" at "http://repo.spray.io/"
    ),
    publishTo := Some("Mesosphere Repository" at "s3://downloads.mesosphere.io/maven")
  )
}

object Dependencies {
  import Dependency._

  val root = Seq(
    // runtime
    akkaActor % "compile",
    akkaSlf4j % "compile",
    sprayClient % "compile",
    sprayHttpx % "compile",
    json4s % "compile",
    chaos % "compile",
    mesosUtils % "compile",
    jacksonCaseClass % "compile",
    mesos % "compile",
    twitterCommons % "compile",
    twitterZkClient % "compile",
    jodaTime % "compile",
    jodaConvert % "compile",

    // test
    Test.scalatest % "test",
    Test.mockito % "test"
  )
}

object Dependency {
  object V {
    // runtime deps versions
    val Chaos = "0.5.6"
    val JacksonCCM = "0.1.0"
    val Mesos = "0.18.2"
    val MesosUtils = "0.18.2-1"
    val Akka = "2.2.4"
    val Spray = "1.2.1"
    val Json4s = "3.2.5"
    val TwitterCommons = "0.0.52"
    val TwitterZkCLient = "0.0.43"
    val JodaTime = "2.3"
    val JodaConvert = "1.5"

    // test deps versions
    val Mockito = "1.9.5"
    val ScalaTest = "2.1.5"
  }

  val akkaActor = "com.typesafe.akka" %% "akka-actor" % V.Akka
  val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % V.Akka
  val sprayClient = "io.spray" % "spray-client" % V.Spray
  val sprayHttpx = "io.spray" % "spray-httpx" % V.Spray
  val json4s = "org.json4s" %% "json4s-jackson" % V.Json4s
  val chaos = "mesosphere" % "chaos" % V.Chaos
  val mesosUtils = "mesosphere" % "mesos-utils" % V.MesosUtils
  val jacksonCaseClass = "mesosphere" %% "jackson-case-class-module" % V.JacksonCCM
  val jodaTime = "joda-time" % "joda-time" % V.JodaTime
  val jodaConvert = "org.joda" % "joda-convert" % V.JodaConvert
  val mesos = "org.apache.mesos" % "mesos" % V.Mesos
  val twitterCommons = "com.twitter.common.zookeeper" % "candidate" % V.TwitterCommons
  val twitterZkClient = "com.twitter.common.zookeeper" % "client" % V.TwitterZkCLient

  object Test {
    val scalatest = "org.scalatest" %% "scalatest" % V.ScalaTest
    val mockito = "org.mockito" % "mockito-all" % V.Mockito
  }
}

