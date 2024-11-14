import jpype
import jpype.dbapi2
import sys
import jpype.imports

jpype.startJVM(classpath=["./target/gridstore-arrow-jdbc-0.1-jar-with-dependencies.jar"])

### For Arrow(事前準備) S
import pyarrow.jvm
from org.apache.arrow.adapter.jdbc import JdbcToArrowConfigBuilder, JdbcToArrow
from org.apache.arrow.memory import RootAllocator

ra = RootAllocator(sys.maxsize)

config_builder = JdbcToArrowConfigBuilder()
config_builder.setAllocator(ra)
config_builder.setTargetBatchSize(2)
pyarrow_jdbc_config = config_builder.build()
### For Arrow(事前準備) E

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

### For Arrow(データ取得処理) S
result_set = curs.resultSet

it = JdbcToArrow.sqlToArrowVectorIterator(result_set, pyarrow_jdbc_config)
while it.hasNext():
    root = it.next()
    if root.getRowCount() == 0:
        break
    x = pyarrow.jvm.record_batch(root)
    print(x)

### For Arrow(データ取得処理) E

curs.close()
conn.close()
print('success!')
