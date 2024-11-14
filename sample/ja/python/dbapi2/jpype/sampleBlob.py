import jpype
import jpype.dbapi2

jpype.startJVM(classpath=['./gridstore-jdbc.jar'])

# Custom S
import jpype.imports
from javax.sql.rowset.serial import SerialBlob
# Custom E

url = "jdbc:gs://127.0.0.1:20001/myCluster/public"
conn = jpype.dbapi2.connect(url, driver="com.toshiba.mwcloud.gs.sql.Driver",
	driver_args={"user":"admin", "password":"admin"})

curs = conn.cursor()

curs.execute("DROP TABLE IF EXISTS SampleBlob")
curs.execute("CREATE TABLE IF NOT EXISTS SampleBlob ( id integer PRIMARY KEY, v1 BLOB )")
print('SQL Create Table name=SampleBlob')

# Custom S
conn._adapters[bytes] = SerialBlob
jpype.dbapi2._default_setters[SerialBlob] = jpype.dbapi2.BLOB
# Custom E

b = "abcde".encode()
curs.execute("INSERT INTO SampleBlob values (?, ?)", (1, b))
b = "xyzvw".encode()
curs.execute("INSERT INTO SampleBlob values (?, ?)", (2, b))
print('SQL Insert')

curs.execute("SELECT * from SampleBlob")
print(curs.fetchall())

curs.close()
conn.close()
print('success!')

