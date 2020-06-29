[JayDeBeApi](https://pypi.org/project/JayDeBeApi/) ([Python DB-API](https://www.python.org/dev/peps/pep-0249/)) Sample for GridDB JDBC Driver

# Operating environment

Python==3.6  
JayDeBeApi==1.2.3  
JPype1==0.6.3

    $ pip install JayDeBeApi
    $ pip install JPype1==0.6.3

# Execute a sample program

GridDB server need to be started in advance with multicast method.

    $ mvn dependency:get -Dartifact=com.github.griddb:gridstore-jdbc:4.5.0.1 -Ddest=./gridstore-jdbc.jar
    $ python DBAPISelect.py <multicast_address> <port_no> <cluster_name> <username> <password>
    SQL Create Table name=Sample
    SQL Insert
    [(3, 'test3'), (4, 'test4')]
    success!
