ThisBuild / organization := "org.quasigroup"
ThisBuild / scalaVersion := "3.10.0"

val FS2Version = "3.14.0"
val WeaverVersion = "0.13.0"
val KittenVersion = "3.5.0"
val CatsMTLVersion = "1.7.0"
val Log4CatsVersion = "2.8.0"
val CatsRetryVersion = "4.0.0"
val CirceVersion = "0.14.17"
val ScalaXMLVersion = "2.5.0"

// Scala 3.10 needs JDK 17 or newer to compile and run.
val MinJavaVersion = 17

lazy val root = (project in file(".")).settings(
  name := "scala-ibclient",
  initialize := {
    val _ = initialize.value
    val javaVersion = sys.props("java.specification.version").split('.').last.toInt // "1.8" -> 8, "21" -> 21
    require(javaVersion >= MinJavaVersion, s"scala-ibclient needs JDK $MinJavaVersion or newer, found $javaVersion")
  },
  libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-xml" % ScalaXMLVersion,
    "org.typelevel" %% "kittens" % KittenVersion,
    "org.typelevel" %% "cats-mtl" % CatsMTLVersion,
    "org.typelevel" %% "log4cats-slf4j" % Log4CatsVersion, // Direct Slf4j Support - Recommended
    "com.github.cb372" %% "cats-retry" % CatsRetryVersion,
    "co.fs2" %% "fs2-core" % FS2Version,
    "co.fs2" %% "fs2-io" % FS2Version,
    "co.fs2" %% "fs2-scodec" % FS2Version,
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion,
    "org.typelevel" %% "weaver-cats" % WeaverVersion % Test,
    "org.typelevel" %% "weaver-scalacheck" % WeaverVersion % Test
  ),
  testFrameworks += new TestFramework("weaver.framework.CatsEffect")
)
