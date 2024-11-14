import jpype
import jpype.dbapi2

jpype.startJVM(classpath=['./gridstore-jdbc.jar'])

url = "jdbc:gs://127.0.0.1:20001/myCluster/public"
conn = jpype.dbapi2.connect(url, driver="com.toshiba.mwcloud.gs.sql.Driver",
	driver_args={"user":"admin", "password":"admin"})

curs = conn.cursor()

curs.execute("DROP TABLE IF EXISTS Sample")
curs.execute("CREATE TABLE IF NOT EXISTS Sample ( id integer PRIMARY KEY, v1 LONG, v2 SHORT, v3 BYTE, v4 BOOL, v5 FLOAT, v6 DOUBLE )")
print('SQL Create Table name=Sample')

curs.execute("INSERT INTO Sample values (0, 1, 2, 3, True, 5, 6)")
print('SQL Insert')

# Custom S
conn._converters[jpype.JByte] = int
conn._converters[jpype.JShort] = int
conn._converters[jpype.JInt] = int
conn._converters[jpype.JLong] = int
conn._converters[jpype.JFloat] = float
conn._converters[jpype.JDouble] = float
# Custom E

curs.execute("SELECT * from Sample")
x = curs.fetchone()
print(x)
print(type(x[0]))
print(type(x[1]))
print(type(x[2]))
print(type(x[3]))
print(type(x[4]))
print(type(x[5]))
print(type(x[6]))

curs.close()
conn.close()
print('success!')

