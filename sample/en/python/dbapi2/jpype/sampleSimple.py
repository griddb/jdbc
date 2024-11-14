import jpype
import jpype.dbapi2

jpype.startJVM(classpath=['./gridstore-jdbc.jar'])

url = "jdbc:gs://127.0.0.1:20001/myCluster/public"
conn = jpype.dbapi2.connect(url, driver="com.toshiba.mwcloud.gs.sql.Driver",
	driver_args={"user":"admin", "password":"admin"})

curs = conn.cursor()

curs.execute("DROP TABLE IF EXISTS Sample")
curs.execute("CREATE TABLE IF NOT EXISTS Sample ( id integer PRIMARY KEY, value string )")
print('SQL Create Table name=Sample')

curs.execute("INSERT INTO Sample values (0, 'test0'),(1, 'test1'),(2, 'test2'),(3, 'test3'),(4, 'test4')")
print('SQL Insert')

curs.execute("SELECT * from Sample where id > 2")
print(curs.fetchall())

curs.close()
conn.close()
print('success!')

