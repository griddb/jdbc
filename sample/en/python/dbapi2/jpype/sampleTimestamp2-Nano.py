import jpype
import jpype.dbapi2
import datetime

jpype.startJVM(classpath=['./gridstore-jdbc.jar'])


url = "jdbc:gs://127.0.0.1:20001/myCluster/public"
conn = jpype.dbapi2.connect(url, driver="com.toshiba.mwcloud.gs.sql.Driver",
	driver_args={"user":"admin", "password":"admin"})

curs = conn.cursor()

curs.execute("DROP TABLE IF EXISTS Sample9")
# TIMESTAMP(9) (nanosecond)
curs.execute("CREATE TABLE IF NOT EXISTS Sample9 ( id integer PRIMARY KEY, t TIMESTAMP(9) )")
print('SQL Create Table name=Sample9')

# TIMESTAMP(9) (nanosecond)
curs.execute("INSERT INTO Sample9 values (1, TIMESTAMP_NS('2024-05-18T12:00:00.123456789Z'))")
print('SQL Insert')

# Custom S
import jpype.imports
import java.sql.Timestamp

conn._converters[java.sql.Timestamp] = jpype.dbapi2._nop
# Custom E

curs.execute("SELECT * from Sample9")
x = curs.fetchone()
print(x[0])
print(x[1])

curs.close()
conn.close()
print('success!')

