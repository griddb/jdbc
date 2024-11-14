[JPype](https://jpype.readthedocs.io/en/latest/) ([Python DB-API](https://www.python.org/dev/peps/pep-0249/)) Sample for GridDB JDBC Driver

# Operating environment and preparation
 
JPype1==1.5.0
pyarrow==16.0.0

    $ pip install JPype1==1.5.0
    $ mvn dependency:get -Dartifact=com.github.griddb:gridstore-jdbc:5.6.0 -Ddest=./gridstore-jdbc.jar

    (Using samples for Apache Arrow)
    $ pip install pyarrow==16.0.0
    $ mvn assembly:single
    $ export _JAVA_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED"

# Execute a sample program

GridDB server need to be started in advance.

    $ python sampleSimple.py

