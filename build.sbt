name := "StructuredStreaming"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.3.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.0.0"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" classifier "models"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
libraryDependencies += "net.liftweb" %% "lift-json" % "3.3.0"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.8"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.8"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0"

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case "about.html" => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

mainClass in assembly := Some("TwitterSentimentsLoad")