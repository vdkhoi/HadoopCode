package de.l3s.local.websci16;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.regex.Pattern;

import org.stringtemplate.v4.compiler.STParser.arg_return;

public class UploadScore2DB {

	public static String score_path = "Z:/Alexandria/Results/Result_follow_up_WebSci16/200_result_score";
	
	public static String entity_mapping_file = "Z:/Alexandria/Results/Result_follow_up_WebSci16/200_entities_list_and_letor_id";
	
	public static TObjectIntHashMap<String> entity_list = new  TObjectIntHashMap<String>();
	
	public static TObjectIntHashMap<String> load_entity_mapping(String path) {
		TObjectIntHashMap<String> temp = new  TObjectIntHashMap<String>();
		File f = new File (path);
		try {
			BufferedReader br = new BufferedReader(new FileReader(f));
			String rl = "";
			String[] fields = null; 
			while ((rl = br.readLine()) != null) {
				fields = rl.split("\t");
				temp.put(fields[1], Integer.parseInt(fields[0]));
			}
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return temp;
	}

	public static void main(String agrs[]) {
		if (agrs.length == 2) {
			score_path = agrs[0];
			entity_mapping_file = agrs[1];
		}
		
		File score_path_dir = new File(score_path); 
		File[] list_file = score_path_dir.listFiles();
		
		
		Connection conn = null;
		PreparedStatement stmt = null;

		
		try {
			DriverManager.registerDriver(new com.mysql.jdbc.Driver());
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection("jdbc:mysql://db.l3s.uni-hannover.de:3306/ttran", "ttran", "DFTKKbGV3bxZZb8C");
			stmt = conn.prepareStatement("INSERT INTO `khoi_search_result_cache` VALUES(?, ?, ?)");

		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		for (int i = 0; i < list_file.length; i++) {
			try {
				entity_list = load_entity_mapping(entity_mapping_file);
				BufferedReader br = new BufferedReader(new FileReader(list_file[i]));
				String rl = "";
				String[] fields = null;
				int count = 0;
				while ((rl = br.readLine()) != null) {
					fields = rl.split("[\t#:|]");
					//System.out.println(fields[0] + "\t" + entity_list.get(fields[3]) + "\t" + fields[5] + "\t" + fields[7]);
					stmt.setLong(1, Long.parseLong(fields[5]));
					stmt.setInt(2, entity_list.get(fields[3]));
					stmt.setDouble(3, Double.parseDouble(fields[0]));
					stmt.addBatch();
					count ++;
					if (count == 10000){
						try {
							System.out.println("Update result: " + fields[3]);
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
