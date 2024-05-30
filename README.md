GridDB JDBC Driver

## Overview

The GridDB JDBC Driver provides an SQL interface for GridDB.  

(Additional information)  
There is [Jar Package for GridDB Community Edition on Maven Central Repository](https://search.maven.org/search?q=a:gridstore-jdbc).
To download the precompiled jars, click on View all to see all versions of the driver, browse, and download gridstore-jdbc-x.x.x.jar from the repository.

## Operating environment

Building and program execution are checked in the environment below.

    OS: CentOS 7.9(x64)
    GridDB server: V5.6 CE(Community Edition)

## Quick start

### Build
    
Run the make command like the following:
    
    $ ant

and create the following file and links under the bin/ folder.
    
    gridstore-jdbc.jar
    gridstore-jdbc-call-logging.jar

### Execute a sample program
GridDB server needs to be started in advance with "myCluster" as a cluster name and multicast method.

    $ export CLASSPATH=${CLASSPATH}:./bin/gridstore-jdbc.jar
    $ cp sample/en/jdbc/JDBCSelect.java .
    $ javac JDBCSelect.java
    $ java JDBCSelect

## Document
  Refer to the file below for more detailed information.  
  - [JDBC Driver UserGuide](https://github.com/griddb/docs-en/blob/master/manuals/GridDB_JDBC_Driver_UserGuide.md)
  - [SQL Reference](https://github.com/griddb/docs-en/blob/master/manuals/GridDB_SQL_Reference.md)

## Community
  * Issues  
    Use the GitHub issue function if you have any requests, questions, or bug reports. 
  * PullRequest  
    Use the GitHub pull request function if you want to contribute code.
    You'll need to agree GridDB Contributor License Agreement(CLA_rev1.1.pdf).
    By using the GitHub pull request function, you shall be deemed to have agreed to GridDB Contributor License Agreement.

## License
  The JDBC Driver source license is Apache License, version 2.0.  
  See 3rd_party/3rd_party.md for the source and license of the third party.
