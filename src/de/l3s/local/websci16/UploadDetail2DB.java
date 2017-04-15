package de.l3s.local.websci16;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;




public class UploadDetail2DB {

	public static String detail_path = "Z:/Alexandria/Results/Result_follow_up_WebSci16/200_result_score";
		
	
	

	public static void main(String agrs[]) {
		if (agrs.length == 1) {
			detail_path = agrs[0];

		}
		
		File score_path_dir = new File(detail_path); 
		File[] list_file = score_path_dir.listFiles();
		
		
		Connection conn = null;
		PreparedStatement stmt = null;

		
		try {
			DriverManager.registerDriver(new com.mysql.jdbc.Driver());
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection("jdbc:mysql://db.l3s.uni-hannover.de:3306/ttran", "ttran", "DFTKKbGV3bxZZb8C");
			stmt = conn.prepareStatement("INSERT INTO `khoi_search_result_detail` VALUES(?, ?, ?, ?)");

		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		for (int i = 0; i < list_file.length; i++) {
			try {
				BufferedReader br = new BufferedReader(new FileReader(list_file[i]));
				String rl = "";
				String[] fields = null;
				int count = 0;
				while ((rl = br.readLine()) != null) {
					fields = rl.split("\t");
					//System.out.println(fields[0] + "\t" + entity_list.get(fields[3]) + "\t" + fields[5] + "\t" + fields[7]);
					stmt.setLong(1, Long.parseLong(fields[0]));
					stmt.setString(2, fields[1]);
					stmt.setString(3, fields[2]);
					stmt.setString(4, fields[3]);
					stmt.addBatch();
					count ++;
					if (count == 10000){
						try {
							stmt.executeBatch();
						}
						catch (SQLException ex) {
							ex.printStackTrace();
						}
						count = 0;
					}
				}
				
				if (count < 10000) {
					try {
						System.out.println("Update result: " + fields[3]);
						stmt.executeBatch();
					}
					catch (SQLException ex) {
						ex.printStackTrace();
					}
				}
				
				br.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
}
