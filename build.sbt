name              := "snowflake-usage-report"
organizationName  := "Guild Education"
organization       := "guild"
description        := "Local Spark tool that pulls Snowflake ACCOUNT_USAGE metadata and generates an offline HTML/Plotly usage report ahead of a Databricks migration"
scalaVersion       := "2.12.18"
version            := "0.1.0"

scalacOptions += "-deprecation"
scalacOptions += "-Ywarn-unused-import"
scalacOptions += "-Ywarn-dead-code"
scalacOptions += "-Ywarn-unused"
scalacOptions += "-Ywarn-value-discard"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"     % sparkVersion,
  "org.apache.spark" %% "spark-sql"      % sparkVersion,
  "net.snowflake"     % "snowflake-jdbc" % "3.20.0",
  "com.databricks"    % "databricks-jdbc" % "2.7.3",
  "org.rogach"       %% "scallop"        % "5.1.0"
)

// Spark 3.5 needs these opens on JDK 17+ to reflectively access java.* internals.
Compile / run / fork := true
javaOptions ++= Seq(
  "-Xmx10g",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)
