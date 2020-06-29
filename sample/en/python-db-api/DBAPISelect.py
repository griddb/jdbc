import jaydebeapi
import sys

argv = sys.argv

multicast_address = argv[1] # default : 239.0.0.1
port_no = argv[2] # default : 41999
cluster_name = argv[3]
username = argv[4]
password = argv[5]

url = "jdbc:gs://" + multicast_address + ":" + port_no + "/" + cluster_name
conn = jaydebeapi.connect("com.toshiba.mwcloud.gs.sql.Driver",
    url, [username, password], "./gridstore-jdbc.jar")

curs = conn.cursor()

curs.execute("DROP TABLE IF EXISTS Sample")
curs.execute("CREATE TABLE IF NOT EXISTS Sample ( id integer PRIMARY KEY, value string )")
print('SQL Create Table name=Sample')
curs.execute("INSERT INTO Sample values (0, 'test0'),(1, 'test1'),(2, 'test2'),(3, 'test3'),(4, 'test4')")
print('SQL Insert')
curs.execute("SELECT * from Sample where ID > 2")
print(curs.fetchall())

curs.close()
conn.close()
print('success!')
