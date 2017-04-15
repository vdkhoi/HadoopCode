package de.l3s.local.websci16;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


public class FetchSpecificEntity {

	public static void main(String agrs[]) {
		try {

			DriverManager.registerDriver(new com.mysql.jdbc.Driver());
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			Connection conn = DriverManager
					.getConnection(
							"jdbc:mysql://prometheus.kbs.uni-hannover.de:3306/archive_bing_big",
							"archive_bing_r", "uxtN99NJ7Wh5J9bN");
			Statement stmt = conn.createStatement();
			File f = new File("Z:/Alexandria/Data_Input/fix_entity.txt");
			BufferedReader br = new BufferedReader(new FileReader(f));
			String rl = null;
			while ((rl = br.readLine()) != null) {
				String[] order = rl.split("\t");
				File o = new File("Z:/Alexandria/Results/query_results/" + order[1]);
				if (o.exists()) continue;
				
				System.out.println("PROCESS: - " + order[1]);
				
				String query = "SELECT u.* "
						+ " FROM main_pages_de q, url_captures_count_2 u "
						+ " WHERE q.query_id = u.query_id "
						+ " AND LOWER(q.title) = 'angela merkel'";
				
				
				ResultSet rs = stmt.executeQuery(query);
				
				BufferedWriter wr = new BufferedWriter(new FileWriter(o));
				while (rs.next()) {
					wr.write(rs.getInt(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3) + "\t" + rs.getString(4) + "\t" + rs.getString(5) + "\t" + rs.getString(6) + "\r\n");
				}
				wr.close();
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
