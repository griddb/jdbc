import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class JDBCConnect {

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

			// (1)クラスタ名とデータベース名はエンコードする
			String encodeClusterName = URLEncoder.encode(clusterName, "UTF-8");
			String encodeDatabaseName = URLEncoder.encode(databaseName, "UTF-8");

			// (2)URLを組み立てる (マルチキャスト方式)
			String jdbcUrl = "jdbc:gs://"+jdbcAddress+":"+jdbcPort+"/"+encodeClusterName+"/"+encodeDatabaseName;

			/*
			// (固定リスト方式の場合)
			// JDBCの接続情報
			String fixedList = "192.168.1.10:20001,192.168.1.11:20001,192.168.1.12:20001";	// ノード3台の例
			String clusterName = "myCluster";
			String databaseName = "public";
			String username = "admin";
			String password = "admin";
			String applicationName = "SampleJDBC";

			// エンコード
			String encodeClusterName = URLEncoder.encode(clusterName, "UTF-8");
			String encodeDatabaseName = URLEncoder.encode(databaseName, "UTF-8");
			String encodeFixedList = URLEncoder.encode(fixedList, "UTF-8");

			// URL
			String jdbcUrl = "jdbc:gs:///" + encodeClusterName + "/" + encodeDatabaseName + "?notificationMember="+encodeFixedList;
			*/
			
			/*
			// (プロバイダ方式の場合)
			// JDBCの接続情報
			String providerUrl = "http://example.com/notification/provider";
			String clusterName = "myCluster";
			String databaseName = "public";
			String username = "admin";
			String password = "admin";
			String applicationName = "SampleJDBC";

			// エンコード
			String encodeClusterName = URLEncoder.encode(clusterName, "UTF-8");
			String encodeDatabaseName = URLEncoder.encode(databaseName, "UTF-8");
			String encodeProviderUrl = URLEncoder.encode(providerUrl, "UTF-8");

			// URL
			String jdbcUrl = "jdbc:gs:///" + encodeClusterName + "/" + encodeDatabaseName + "?notificationProvider="+encodeProviderUrl;
			*/
			
			Properties prop = new Properties();
			prop.setProperty("user", username);
			prop.setProperty("password", password);
			prop.setProperty("applicationName", applicationName);

			// (3)接続する
			Connection con = DriverManager.getConnection(jdbcUrl, prop);

			System.out.println("Connect to cluster");

			//===============================================
			// 終了処理
			//===============================================
			// (4)接続をクローズする
			con.close();
			System.out.println("success!");

		} catch ( Exception e ){
			e.printStackTrace();
		}
	}
}