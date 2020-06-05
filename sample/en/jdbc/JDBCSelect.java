import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class JDBCSelect {

    public static void main(String[] args){
        try {
            //===============================================
            // Connecting to a cluster
            //===============================================
            // Connection information of JDBC
            String jdbcAddress = "239.0.0.1";
            String jdbcPort = "41999";
            String clusterName = "myCluster";
            String databaseName = "public";
            String username = "admin";
            String password = "admin";
            String applicationName = "SampleJDBC";

            // Encoding a cluster name and a database name
            String encodeClusterName = URLEncoder.encode(clusterName, "UTF-8");
            String encodeDatabaseName = URLEncoder.encode(databaseName, "UTF-8");

            // Creating a URL (Multicast method)
            String jdbcUrl = "jdbc:gs://"+jdbcAddress+":"+jdbcPort+"/"+encodeClusterName+"/"+encodeDatabaseName;

            Properties prop = new Properties();
            prop.setProperty("user", username);
            prop.setProperty("password", password);
            prop.setProperty("applicationName", applicationName);

            // Connecting
            Connection con = DriverManager.getConnection(jdbcUrl, prop);

            //===============================================
            // Creating a container and registering data
            //===============================================
            {
                Statement stmt = con.createStatement();

                // (Delete the container if it already exists)
                stmt.executeUpdate("DROP TABLE IF EXISTS SampleJDBC_Select");

                // Creating a container
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS SampleJDBC_Select ( id integer PRIMARY KEY, value string )");
                System.out.println("SQL Create Table name=SampleJDBC_Select");

                // Registering a row
                int ret1 = stmt.executeUpdate("INSERT INTO SampleJDBC_Select values "+
                                            "(0, 'test0'),(1, 'test1'),(2, 'test2'),(3, 'test3'),(4, 'test4')");
                System.out.println("SQL Insert count=" + ret1);

                stmt.close();
            }

            //===============================================
            // Executing a search
            //===============================================
            // (1) Creating a statement
            Statement stmt = con.createStatement();

            // (2) Executing a SQL command
            ResultSet rs = stmt.executeQuery("SELECT * from SampleJDBC_Select where ID > 2");

            // (3) Getting a result
            while( rs.next() ){
                int id = rs.getInt(1);
                String value = rs.getString(2);
                System.out.println("SQL row(id=" + id + ", value=" + value + ")");
            }

            //===============================================
            // Terminating process
            //===============================================
            stmt.close();
            con.close();
            System.out.println("success!");

        } catch ( Exception e ){
            e.printStackTrace();
        }
    }
}