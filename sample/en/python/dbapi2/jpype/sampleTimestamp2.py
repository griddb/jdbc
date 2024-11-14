import jpype
import jpype.dbapi2
import datetime

jpype.startJVM(classpath=['./gridstore-jdbc.jar'])

url = "jdbc:gs://127.0.0.1:20001/myCluster/public"
conn = jpype.dbapi2.connect(url, driver="com.toshiba.mwcloud.gs.sql.Driver",
	driver_args={"user":"admin", "password":"admin"})

curs = conn.cursor()

curs.execute("DROP TABLE IF EXISTS Sample")
curs.execute("DROP TABLE IF EXISTS Sample6")
curs.execute("DROP TABLE IF EXISTS Sample9")
# TIMESTAMP (milisecond)
curs.execute("CREATE TABLE IF NOT EXISTS Sample ( id integer PRIMARY KEY, t TIMESTAMP )")
# TIMESTAMP(6) (microsecond)
curs.execute("CREATE TABLE IF NOT EXISTS Sample6 ( id integer PRIMARY KEY, t TIMESTAMP(6) )")
# TIMESTAMP(9) (nanosecond)
curs.execute("CREATE TABLE IF NOT EXISTS Sample9 ( id integer PRIMARY KEY, t TIMESTAMP(9) )")
print('SQL Create Table name=Sample/Sample6/Sample9')

# TIMESTAMP (milisecond)
curs.execute("INSERT INTO Sample values (0, TIMESTAMP('2024-05-18T12:00:00Z'))")
curs.execute("INSERT INTO Sample values (1, TIMESTAMP_MS('2024-05-18T12:00:00.123Z'))")
print('SQL Insert')
curs.execute("SELECT * from Sample")
print(curs.fetchall())

# TIMESTAMP(6) (microsecond)
curs.execute("INSERT INTO Sample6 values (1, TIMESTAMP_US('2024-05-18T12:00:00.123456Z'))")
print('SQL Insert')
curs.execute("SELECT * from Sample6")
print(curs.fetchall())

# TIMESTAMP(9) (nanosecond)
curs.execute("INSERT INTO Sample9 values (1, TIMESTAMP_NS('2024-05-18T12:00:00.123456789Z'))")
print('SQL Insert')
curs.execute("SELECT * from Sample9")
print(curs.fetchall())

curs.close()
conn.close()
print('success!')

