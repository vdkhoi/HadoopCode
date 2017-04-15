package de.l3s.local.websci16;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class FetchFullEntityList {

	public static void main(String agrs[]) {
		try {

			DriverManager.registerDriver(new com.mysql.jdbc.Driver());
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			Connection conn = DriverManager
					.getConnection(
							"jdbc:mysql://prometheus.kbs.uni-hannover.de:3306/archive_bing_big",
							"archive_bing_r", "uxtN99NJ7Wh5J9bN");
			Statement stmt = conn.createStatement();

			String query = "SELECT DISTINCT title " + " FROM main_pages_de";

			ResultSet rs = stmt.executeQuery(query);

			File o = new File(
					"Z:/Alexandria/Results/Result_follow_up_WebSci16/entity_full_list.txt");
			BufferedWriter wr = new BufferedWriter(new FileWriter(o));
			GermanAnalyzer analyzer = new GermanAnalyzer();
			while (rs.next()) {
				TokenStream tokens = analyzer.tokenStream(null, rs.getString(1));
				CharTermAttribute charTermAttrib = tokens
						.getAttribute(CharTermAttribute.class);
				tokens.reset();
				String ent = "";
				while (tokens.incrementToken()) {
					String term = charTermAttrib.toString();
					if (term.length() > 2 && !term.matches("^[-+]?\\d+([\\.\\,]\\d+)*$")) {
						ent += (term + " ");
					}
				}
				if (ent.length() > 1) {
					ent = ent.substring(0, ent.length() - 1);
					wr.write(ent + "\t");
				}
				tokens.end();
				tokens.close();
			}
			analyzer.close();
			wr.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
