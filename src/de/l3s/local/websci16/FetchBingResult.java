package de.l3s.local.websci16;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.regex.Pattern;

public class FetchBingResult {

	public static boolean checkDomainDe (String arc_url) {
		if (arc_url.indexOf("http://www.") == 0)
			arc_url = arc_url.replaceFirst(
					Pattern.quote("http://www."), "");

		if (arc_url.indexOf("https://www.") == 0)
			arc_url = arc_url.replaceFirst(
					Pattern.quote("https://www."), "");

		if (arc_url.indexOf("http://") == 0)
			arc_url = arc_url.replaceFirst(
					Pattern.quote("http://"), "");

		if (arc_url.indexOf("https://") == 0)
			arc_url = arc_url.replaceFirst(
					Pattern.quote("https://"), "");

		if (arc_url.substring(arc_url.indexOf('/') - 2,
				arc_url.indexOf('/')).equals("de")) {
			return true;
		}
		return false;
	}
	
	public static boolean checkValidTimestamp (String arc_timestamp) {
		if (arc_timestamp != null) {
			if (arc_timestamp.compareTo("null") != 0 || arc_timestamp.length() > 0) {
				if (arc_timestamp.compareTo("2014-01-01 00:00:00.0") < 0) {
					return true;
				}
			}
		}
		return false;
	}

	
	public static void main(String agrs[]) {
		try {
			// query_entity = agrs[0];
			// query_file = agrs[1];

			DriverManager.registerDriver(new com.mysql.jdbc.Driver());
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			Connection conn = DriverManager
					.getConnection(
							"jdbc:mysql://prometheus.kbs.uni-hannover.de:3306/archive_bing_big",
							"archive_bing_r", "uxtN99NJ7Wh5J9bN");
			Statement stmt = conn.createStatement();
			InputStream f = new FileInputStream("Z:/Alexandria/Data_Input/fix_entity.txt");
			BufferedReader br = new BufferedReader(new InputStreamReader(f, "ISO-8859-1"));
			String rl = null;
			while ((rl = br.readLine()) != null) {
				if (rl.startsWith("#")) continue;
				String[] order = rl.split("\t");
				File o = new File("Z:/Alexandria/Results/Result_follow_up_WebSci16/200_bing_result_have_archived/" + order[1]);
				//if (o.exists()) continue;
				
				//System.out.println("PROCESS: - " + order[1]);
				
				
				
				String query = "SELECT DISTINCT r.rank, r.url, u.url_captures, u.first_timestamp "
						+ " FROM main_pages_de q, pw_result r, url_captures_count_2 u "
						+ " WHERE q.query_id = r.query_id "
						+ " AND u.query_id = r.query_id "
						+ " AND u.rank = r.rank "
//						+ " AND LOWER(q.title) LIKE LOWER('%" + order[0].replace(' ', '%') + "%')"
						+ " AND LOWER(q.title) = LOWER('" + order[0] + "')"
						+ " ORDER BY rank " ;
//						+ " LIMIT 0 , 100 ";
				System.out.println("PROCESS: - " + order[1] + " - " + query);
				
				ResultSet rs = stmt.executeQuery(query);
				
				BufferedWriter wr = new BufferedWriter(new FileWriter(o));
				while (rs.next()) {
					if (checkDomainDe(rs.getString(2)) && checkValidTimestamp(rs.getString(4))) {
						wr.write(rs.getInt(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3) + "\t" + rs.getString(4) + "\r\n");
					}
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
