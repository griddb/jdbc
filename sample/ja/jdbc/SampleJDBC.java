// 本サンプルプログラムはGridDB Advanced Editionのサンプルプログラムです。
// sample2が実行されている必要があります。
package test;

import java.sql.*;

public class SampleJDBC {
	public static void main(String[] args) throws SQLException {
		if (args.length != 5) {
			System.err.println(
				"usage: java SampleJDBC (multicastAddress) (port) (clusterName) (user) (password)");
			System.exit(1);
		}

		// urlは"jdbc:gs://(multicastAddress):(portNo)/(clusterName)"形式
		String url = "jdbc:gs://" + args[0] + ":" + args[1] + "/" + args[2];
		String user = args[3];
		String password = args[4];

		System.out.println("DB Connection Start");

		// GridDBクラスタとの接続
		Connection con = DriverManager.getConnection(url, user, password);
		try {
			System.out.println("Start");
			Statement st = con.createStatement();
			ResultSet rs = st.executeQuery("SELECT * FROM point01");
			ResultSetMetaData md = rs.getMetaData();
			while (rs.next()) {
				for (int i = 0; i < md.getColumnCount(); i++) {
					System.out.print(rs.getString(i + 1) + "|");
				}
				System.out.println("");
			}
			rs.close();
			System.out.println("End");
			st.close();
		} finally {
			System.out.println("DB Connection Close");
			con.close();
		}
	}
}