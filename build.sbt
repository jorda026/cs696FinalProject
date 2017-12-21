name := "CS696FinalProject"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.2.0",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0",
  "org.apache.kafka" % "kafka-clients" % "1.0.0",
  "org.apache.kafka" % "kafka_2.11" % "1.0.0"
      exclude("javax.jms", "jms")
      exclude("com.sun.jdmk", "jmxtools")
      exclude("com.sun.jmx", "jmxri")

)
resolvers += Resolver.sonatypeRepo("releases")

        