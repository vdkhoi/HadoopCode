package de.l3s.local.websci16;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class UpdateQueryRank2DB {
	public static void main(String agrs[]) {
		Connection conn = null;
		PreparedStatement stmt = null;
		Statement retrieve_query = null;
		ResultSet rs = null;

		try {
			DriverManager.registerDriver(new com.mysql.jdbc.Driver());
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection(
					"jdbc:mysql://db.l3s.uni-hannover.de:3306/ttran", "ttran",
					"DFTKKbGV3bxZZb8C");
			retrieve_query = conn.createStatement();
			stmt = conn
					.prepareStatement("UPDATE khoi_search_result_cache SET query_rank=? WHERE doc_id=? AND query_id=?");

			for (int query_id = 1; query_id <= 200; query_id++) {
				rs = retrieve_query
						.executeQuery("SELECT doc_id, match_score, query_id FROM khoi_search_result_cache" 
								+ " WHERE query_id = " + query_id + " ORDER BY match_score DESC");
				int curr_rank = 1;
				while (rs.next()) {
					stmt.setInt(1, curr_rank++);
					stmt.setLong(2, rs.getLong(1));
					stmt.setInt(3, rs.getInt(3));
					stmt.addBatch();
				}
				stmt.executeBatch();
				rs.close();
			}
			stmt.close();
			conn.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
