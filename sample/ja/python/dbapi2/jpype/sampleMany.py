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

data = [
    (0, 'test0'),
    (1, 'test1'),
    (2, 'test2')
]

curs.executemany("INSERT INTO Sample values (?, ?)", data)
print('SQL Insert')

curs.execute("SELECT * from Sample")
print(curs.fetchall())

curs.close()
conn.close()
print('success!')

