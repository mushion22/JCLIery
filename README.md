JCLIery
=======
### About
JCLIery is a simple Java utility to execute SQL commands using JDBC connections to a database. It reads in individual commands/queries stored in 1 or more files and executes them individually, writing the results to the console. The results will either be a CSV formatted list of records returned (eg a SELECT query), or a count of rows affected in the case no results are returned (eg an UPDATE or DDL command)

Put the JDBC driver library for your database on the classpath. This means either alongside the jar executable, or use JVM arguments to specify an additional classpath. 
### Usage
`java -jar JCLiery.jar [options] file1.sql file2.sql ... fileN.sql`



