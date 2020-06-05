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
			// クラスタに接続する
			//===============================================
			// JDBCの接続情報
			String jdbcAddress = "239.0.0.1";
			String jdbcPort = "41999";
			String clusterName = "myCluster";
			String databaseName = "public";
			String username = "admin";
			String password = "admin";
			String applicationName = "SampleJDBC";

			// クラスタ名とデータベース名はエンコードする
			String encodeClusterName = URLEncoder.encode(clusterName, "UTF-8");
			String encodeDatabaseName = URLEncoder.encode(databaseName, "UTF-8");

			// URLを組み立てる (マルチキャスト方式)
			String jdbcUrl = "jdbc:gs://"+jdbcAddress+":"+jdbcPort+"/"+encodeClusterName+"/"+encodeDatabaseName;

			Properties prop = new Properties();
			prop.setProperty("user", username);
			prop.setProperty("password", password);
			prop.setProperty("applicationName", applicationName);

			// 接続する
			Connection con = DriverManager.getConnection(jdbcUrl, prop);

			//===============================================
			// コンテナ作成とデータ登録
			//===============================================
			{
				Statement stmt = con.createStatement();

				// (存在した場合は削除する)
				stmt.executeUpdate("DROP TABLE IF EXISTS SampleJDBC_Select");
				
				// コンテナ作成
				stmt.executeUpdate("CREATE TABLE IF NOT EXISTS SampleJDBC_Select ( id integer PRIMARY KEY, value string )");
				System.out.println("SQL Create Table name=SampleJDBC_Select");

				// ロウ登録
				int ret1 = stmt.executeUpdate("INSERT INTO SampleJDBC_Select values "+
											"(0, 'test0'),(1, 'test1'),(2, 'test2'),(3, 'test3'),(4, 'test4')");
				System.out.println("SQL Insert count=" + ret1);
				
				stmt.close();
			}

			//===============================================
			// 検索の実行
			//===============================================
			// (1)ステートメント作成
			Statement stmt = con.createStatement();

			// (2)SQL実行
			ResultSet rs = stmt.executeQuery("SELECT * from SampleJDBC_Select where ID > 2");

			// (3)結果の取得
			while( rs.next() ){
				int id = rs.getInt(1);
				String value = rs.getString(2);
				System.out.println("SQL row(id=" + id + ", value=" + value + ")");
			}

			//===============================================
			// 終了処理
			//===============================================
			stmt.close();
			con.close();
			System.out.println("success!");

		} catch ( Exception e ){
			e.printStackTrace();
		}
	}
}