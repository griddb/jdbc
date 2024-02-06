import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class JDBCAddBatch {

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
			// コンテナ作成
			//===============================================
			Statement stmt = con.createStatement();

			// (存在した場合は削除する)
			stmt.executeUpdate("DROP TABLE IF EXISTS SampleJDBC_AddBatch");
			
			// コンテナ作成
			stmt.executeUpdate("CREATE TABLE SampleJDBC_AddBatch ( id integer PRIMARY KEY, value string )");
			System.out.println("SQL Create Table name=SampleJDBC_AddBatch");

			stmt.close();

			//===============================================
			// データ登録
			//===============================================
			// (1)プリペアードステートメント作成
			String sql = "insert into SampleJDBC_AddBatch (id, value) Values(?,?)";
			PreparedStatement pstmt = con.prepareStatement(sql);

			// (2)値を設定する
			pstmt.setInt(1,1);
			pstmt.setString(2,"test0");
			pstmt.addBatch();
			pstmt.setInt(1,2);
			pstmt.setString(2,"test1");
			pstmt.addBatch();

			// (3)SQL実行
			int[] cnt = pstmt.executeBatch();

			//===============================================
			// 終了処理
			//===============================================
			pstmt.close();
			con.close();
			System.out.println("success!");

		} catch ( Exception e ){
			e.printStackTrace();
		}
	}
}