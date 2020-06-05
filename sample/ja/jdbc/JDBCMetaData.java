import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class JDBCMetaData {

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
			{
				Statement stmt = con.createStatement();

				// (存在した場合は削除する)
				ResultSet rs = stmt.executeQuery("SELECT * FROM \"#index_info\" WHERE DATABASE_NAME='public' AND TABLE_NAME='SampleJDBC_Meta1' AND INDEX_NAME='index1'");
				if ( rs.next() ){
					stmt.executeUpdate("DROP INDEX IF EXISTS index1 ON SampleJDBC_Meta1");
				}
				rs.close();
				
				stmt.executeUpdate("DROP TABLE IF EXISTS SampleJDBC_Meta1");
				stmt.executeUpdate("DROP TABLE IF EXISTS SampleJDBC_Meta2");
				
				stmt.executeUpdate("CREATE TABLE SampleJDBC_Meta1 ( id integer, value double NOT NULL )");
				stmt.executeUpdate("CREATE INDEX index1 ON SampleJDBC_Meta1 (value)");
				stmt.executeUpdate("CREATE TABLE SampleJDBC_Meta2 ( id integer PRIMARY KEY, status byte )");
				
				stmt.close();
			}

			//===============================================
			// DatabaseMetaData
			//===============================================
			{
				DatabaseMetaData meta = con.getMetaData();

				//###########################################
				// getTables
				//###########################################
				// コンテナ一覧を取得する
				System.out.println("DatabaseMetadata.getTables  null");

				ResultSet rs = meta.getTables(null, null, null, null);
				while( rs.next() ){
					String name = rs.getString("TABLE_NAME");
					String type = rs.getString("TABLE_TYPE");
					System.out.println("  (name=" + name + ", type=" + type +")");
				}
				rs.close();

				System.out.println("------------------------------------------");

				// コンテナSampleJDBC_Meta1のコンテナ一覧を取得する
				System.out.println("DatabaseMetadata.getTables  SampleJDBC_Meta1");

				rs = meta.getTables(null, null, "SampleJDBC_Meta1", null);
				while( rs.next() ){
					String name = rs.getString("TABLE_NAME");
					String type = rs.getString("TABLE_TYPE");
					System.out.println("  (name=" + name + ", type=" + type +")");
				}
				rs.close();

				System.out.println("------------------------------------------");
				// コンテナ名が「SampleJDBC_」で始まるコンテナ一覧を取得する
				System.out.println("DatabaseMetadata.getTables SampleJDBC\\_%");

				rs = meta.getTables(null, null, "SampleJDBC\\_%", null);
				while( rs.next() ){
					String name = rs.getString("TABLE_NAME");
					String type = rs.getString("TABLE_TYPE");
					System.out.println("  (name=" + name + ", type=" + type +")");
				}
				rs.close();

				System.out.println("------------------------------------------");


				//###########################################
				// getColumns
				//###########################################
				// 全コンテナのカラム情報の全カラムの表示
				System.out.println("DatabaseMetadata.getColumns null");

				rs = meta.getColumns(null, null, null, null);
				while( rs.next() ){
					// 全カラム取得
					String catalog = rs.getString("TABLE_CAT");
					String schem = rs.getString("TABLE_SCHEM");
					String name = rs.getString("TABLE_NAME");
					String columnName = rs.getString("COLUMN_NAME");
					String columnDataType = rs.getString("DATA_TYPE");
					String columnTypeName = rs.getString("TYPE_NAME");
					int columnSize = rs.getInt("COLUMN_SIZE");
					int bufferLength = rs.getInt("BUFFER_LENGTH");
					int decimalDigits = rs.getInt("DECIMAL_DIGITS");
					int numPrecradix = rs.getInt("NUM_PREC_RADIX");
					int nullable = rs.getInt("NULLABLE");
					String remarks = rs.getString("REMARKS");
					String columnDef = rs.getString("COLUMN_DEF");
					int sqlDataType = rs.getInt("SQL_DATA_TYPE");
					int sqlDataTimeSub = rs.getInt("SQL_DATETIME_SUB");
					int charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
					int ordinalPosition = rs.getInt("ORDINAL_POSITION");
					String isNullable = rs.getString("IS_NULLABLE");
					String scopeCatalog = rs.getString("SCOPE_CATALOG");
					String scopeSchema = rs.getString("SCOPE_SCHEMA");
					String scopeTable = rs.getString("SCOPE_TABLE");
					short sourceDatatype = rs.getShort("SOURCE_DATA_TYPE");
					String isAutoIncrement = rs.getString("IS_AUTOINCREMENT");
					String isGeneratedColumn = rs.getString("IS_GENERATEDCOLUMN");

					System.out.println("  ("
							+ "tableName=" + name + ", columnName=" + columnName
							+ ", dataType=" + columnDataType + ", typeName=" + columnTypeName
							+ ", nullable="+nullable+", isNullable="+isNullable+")");
				}
				rs.close();

				System.out.println("------------------------------------------");


				// コンテナ"SampleJDBC_Meta1"のカラム情報を取得する
				System.out.println("DatabaseMetadata.getColumns  SampleJDBC_Meta1");

				rs = meta.getColumns(null, null, "SampleJDBC_Meta1", null);
				while( rs.next() ){
					String name = rs.getString("TABLE_NAME");
					String columnName = rs.getString("COLUMN_NAME");
					int columnDataType = rs.getInt("DATA_TYPE");
					String columnTypeName = rs.getString("TYPE_NAME");
					int nullable = rs.getInt("NULLABLE");
					String isNullable = rs.getString("IS_NULLABLE");
					System.out.println("  (name=" + name + ", columnName=" + columnName
							+ ", dataType=" + columnDataType + ", typeName=" + columnTypeName
							+ ", nullable="+nullable+", isNullable="+isNullable+")");
				}
				rs.close();

				System.out.println("------------------------------------------");

				// コンテナ名が「SampleJDBC_」で始まるコンテナのカラム情報を取得する
				System.out.println("DatabaseMetadata.getColumns  SampleJDBC\\_%");

				rs = meta.getColumns(null, null, "SampleJDBC\\_%", null);
				while( rs.next() ){
					String name = rs.getString("TABLE_NAME");
					String columnName = rs.getString("COLUMN_NAME");
					int columnDataType = rs.getInt("DATA_TYPE");
					String columnTypeName = rs.getString("TYPE_NAME");
					int nullable = rs.getInt("NULLABLE");
					String isNullable = rs.getString("IS_NULLABLE");
					System.out.println("  (name=" + name + ", columnName=" + columnName
							+ ", dataType=" + columnDataType + ", typeName=" + columnTypeName
							+ ", nullable="+nullable+", isNullable="+isNullable+")");
				}
				rs.close();

				System.out.println("------------------------------------------");


				//###########################################
				// getPrimaryKeys
				//###########################################
				// 全コンテナのロウキー情報を取得する
				System.out.println("DatabaseMetadata.getPrimaryKeys null");

				rs = meta.getPrimaryKeys(null, null, null);
				while( rs.next() ){
					String name = rs.getString("TABLE_NAME");
					String columnName = rs.getString("COLUMN_NAME");
					short keySequence = rs.getShort("KEY_SEQ");
					System.out.println("  (name=" + name + ", columnName=" + columnName
							+ ", keySequence=" + keySequence + ")");
				}
				rs.close();

				System.out.println("------------------------------------------");

				// コンテナ"SampleJDBC_Meta2"のロウキー情報を取得する
				System.out.println("DatabaseMetadata.getPrimaryKeys SampleJDBC_Meta2");

				rs = meta.getPrimaryKeys(null, null, "SampleJDBC_Meta2");
				while( rs.next() ){
					String name = rs.getString("TABLE_NAME");
					String columnName = rs.getString("COLUMN_NAME");
					short keySequence = rs.getShort("KEY_SEQ");
					System.out.println("  (name=" + name + ", columnName=" + columnName
							+ ", keySequence=" + keySequence + ")");
				}
				rs.close();

				System.out.println("------------------------------------------");


				//###########################################
				// getIndexInfo
				//###########################################
				// コンテナの索引情報を取得する
				System.out.println("DatabaseMetadata.getIndexInfo null");

				rs = meta.getIndexInfo(null, null, null, false, false);
				while( rs.next() ){
					String name = rs.getString("TABLE_NAME");
					String indexName = rs.getString("INDEX_NAME");
					String type = rs.getString("TYPE");
					String columnName = rs.getString("COLUMN_NAME");
					System.out.println("  (name=" + name + ", indexName=" + indexName
							+ ", type=" + type + ", columnName=" + columnName+")");
				}
				rs.close();

				System.out.println("------------------------------------------");

				// コンテナ"SampleJDBC_Meta1"の索引情報を取得する
				System.out.println("DatabaseMetadata.getIndexInfo SampleJDBC_Meta1");

				rs = meta.getIndexInfo(null, null, "SampleJDBC_Meta1", false, false);
				while( rs.next() ){
					String name = rs.getString("TABLE_NAME");
					String indexName = rs.getString("INDEX_NAME");
					String type = rs.getString("TYPE");
					String columnName = rs.getString("COLUMN_NAME");
					System.out.println("  (name=" + name + ", indexName=" + indexName
							+ ", type=" + type + ", columnName=" + columnName+")");
				}
				rs.close();

				System.out.println("------------------------------------------");


				//###########################################
				// getTableTypes
				//###########################################
				// DatabaseMetadata.getTableTypesの全カラムの表示
				System.out.println("DatabaseMetadata.getTableTypes");
				rs = meta.getTableTypes();
				while( rs.next() ){
					String type = rs.getString("TABLE_TYPE");
					System.out.println("  ("
							+ "type=" + type + ")");
				}
				rs.close();

				System.out.println("------------------------------------------");

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
