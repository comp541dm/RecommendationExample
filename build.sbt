name := "RecommendationExample"

version := "1.0"

scalaVersion := "2.11.8"
//libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.4.1"
libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.11" % "1.4.1",
  "org.apache.spark" % "spark-mllib_2.11" % "1.4.1")
