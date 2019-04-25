name := "kafka"

version := "0.1"

scalaVersion := "2.11.12"


//val sparkVersion = "2.3.1"
//val sparkVersion = "2.2.2"
val sparkVersion = "2.4.0"


lazy val root = (project in file(".")).
  settings(
    name := "kafka",
    version := "1.0",
    scalaVersion := "2.12.8",
    mainClass in Compile := Some("kafka")
  )


libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.11.12" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  //"org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0" % "provided",
  //"org.apache.kafka" %% "kafka-clients" % "2.4.0",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0" % "provided",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp")),
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
  //"org.apache.spark" %% "kafka-clients" % "0.11.0.1"
  //"org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.1",
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}