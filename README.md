JCLIery
=======
### About
JCLIery is a simple Java utility to execute SQL commands using JDBC connections to a database. It reads in individual commands/queries stored in 1 or more files and executes them individually, writing the results to the console. The results will either be a CSV formatted list of records if a result set is returned (eg from a SELECT query), or a count of rows affected in the case no results are returned (eg an UPDATE or DDL command)

Currently only supports having one SQL statement per file. Doesn't support comments etc in the file. 

A single connection/session is used to execute all the queries. See the options below to set options for commiting transactions. 

### Usage
Download a .zip with the app code and libraries from the `dist` folder  (https://github.com/mushion22/JCLIery/raw/master/dist/JCLIery-0.1-dist.zip). 

Add the JDBC driver library for your database to the classpath. This means either copy it to the lib folder, or use JVM arguments to specify an additional classpath.

Execute the jar as described below:

    java [JVM Args] [-classpath /path/to/jdbcdriver.jar] -jar JCLiery-<version>.jar [options] file1.sql file2.sql ... fileN.sql
    
    options:
    -c,--nocolumns            Optional    Flag to turn off the printing of column names in the output of results
    -d,--dburl <URL>          Mandatory   JDBC URL for the database. Eg jdbc:mysql://HOST:3306/DATABASE
    -p,--password <Password>  Optional    Password for the database
    -q,--quiet                Optional    Turns off logging output, only outputting results.
    -t,--tx <TYPE>            Optional    Transaction setting:  NONE  (default) no transactions used
                                                                ALL   Commit after all statements executed
                                                                IDV   Commit after each statment is executed
    -u,--username <Username>  Optional    Username for the database
For example

    java -jar JCLIery-0.1.jar -d jdbc:mysql://mysqldb.company.com:3306/mydb -u root -p P@ssw0rd! -t ALL myQ.sql

You might also need to add memory flags if your queries return many results, eg

    java -Xmx1024m -jar JCLIery-0.1.jar -d jdbc:mysql://mysqldb.company.com:3306/mydb -u root -p P@ssw0rd! -t ALL myQ.sql
### Todo
* Add ability to pipe/read commands from the CLI, eg `cat cmd.sql | java -jar JCLIery.jar`
* Add ability to have multiple commands within one file
* Add ability to have comments in file
* Leave password flag argument blank to prompt from STDIN


