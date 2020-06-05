import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.rowset.serial.SerialBlob;


public class JDBCBlobData {

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
				// バイナリデータを格納するコンテナを作成する
				Statement stmt = con.createStatement();
				
				// (存在した場合は削除する)
				stmt.executeUpdate("DROP TABLE IF EXISTS SampleJDBC_BlobData");
				
				stmt.executeUpdate("CREATE TABLE SampleJDBC_BlobData ( id integer PRIMARY KEY, data blob )");
				System.out.println("SQL Create Table name=SampleJDBC_BlobData");
				stmt.close();

				// ファイルからバイナリデータを読み込み登録する
				// (1)プリペアードステートメントを作成する
				PreparedStatement pstmt = con.prepareStatement("INSERT INTO SampleJDBC_BlobData(id, data) VALUES(?, ?)");

				// (2)ファイルからバイナリファイルを読み込む
				File file = new File("JDBCBlobData.class");
				FileInputStream fis = new FileInputStream(file);
			    byte[] b = new byte[1];
			    ByteArrayOutputStream baos = new ByteArrayOutputStream();
			    while (fis.read(b) > 0) {
			        baos.write(b);
			    }
			    baos.close();
			    fis.close();
			    b = baos.toByteArray();

			    // (3)BLOBをセットする
				SerialBlob serialBlob = new SerialBlob(b);
				pstmt.setBlob(2,  serialBlob);
				pstmt.setInt(1, 0);

				// (4)ロウを登録する
				pstmt.executeUpdate();
				pstmt.close();

				System.out.println("Insert Row (Binary)");
			}
			// バイナリデータを取得する
			{
				// (1)検索を実行する
				Statement stmt = con.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT * FROM SampleJDBC_BlobData WHERE id = 0");

				while( rs.next() ){
					// (2)バイナリデータを取得してファイルに保存する
					InputStream input = rs.getBinaryStream(2);
					FileOutputStream output = new FileOutputStream("jdbc_output");
					int c;
					while((c=input.read())!=-1){
						output.write(c);
					}
					input.close();
					output.close();
				}

				rs.close();
				stmt.close();
				System.out.println("Get Row (Binary)");
			}

			//===============================================
			// 終了処理
			//===============================================
			con.close();
			System.out.println("success!");

		} catch ( Exception e ){
			e.printStackTrace();
		}
	}
}