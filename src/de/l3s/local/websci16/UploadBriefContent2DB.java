package de.l3s.local.websci16;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Pattern;

import com.kohlschutter.boilerpipe.document.TextDocument;
import com.kohlschutter.boilerpipe.extractors.ArticleExtractor;
import com.kohlschutter.boilerpipe.sax.BoilerpipeSAXInput;
import com.kohlschutter.boilerpipe.sax.HTMLDocument;
import com.kohlschutter.boilerpipe.sax.HTMLFetcher;

public class UploadBriefContent2DB {

	public static String replaceStr = "+";
	public static int offsetRecord = 0;
	public static int numOfRecord = 0;
	

	public static String getDest_url(String dest_url) {
		String decodeURL = "";

		try {
			decodeURL = URLDecoder.decode(dest_url, "UTF-8");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Error URL: - " + dest_url);
			e.printStackTrace();
		}

		String first = "", later = "", revfirst = "";
		int split_pos = decodeURL.indexOf(')');
		if (split_pos >= 4) {
			first = decodeURL.substring(0, split_pos);
			String[] dn = first.split(",");
			int len = dn.length;
			revfirst = dn[0];
			for (int i = 1; i < len; i++) {
				revfirst = dn[i] + "." + revfirst;
			}
			later = decodeURL.substring(split_pos + 1);
		}
		return (revfirst + later).toLowerCase();
	}
	
	public static String get_url_in_archive_dot_org(String dest_url, String timestamp, String replaceStr) {
		String[] time_part = timestamp.split("[-T:.]");
		String archive_url = "http://web.archive.org/web/" + time_part[0] 
						+ time_part[1] + time_part[2] + time_part[3] 
						+ time_part[4] + time_part[5] + "/" + dest_url.replaceAll(Pattern.quote(" "), replaceStr);
		return archive_url;
	}

	public static void main(String agrs[]) {
		if (agrs.length == 3) {
			replaceStr = agrs[0];
			offsetRecord = Integer.parseInt(agrs[1]);
			numOfRecord = Integer.parseInt(agrs[2]);
		}

		Connection conn = null;
		PreparedStatement stmt = null;
		Statement retrieve_query = null;
		ResultSet rs = null;
		URL url = null;
		URLConnection url_conn = null;
		BufferedReader url_br = null;

		try {
			DriverManager.registerDriver(new com.mysql.jdbc.Driver());
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection(
					"jdbc:mysql://db.l3s.uni-hannover.de:3306/ttran", "ttran",
					"DFTKKbGV3bxZZb8C");
			retrieve_query = conn.createStatement();
			stmt = conn
					.prepareStatement("UPDATE khoi_search_result_detail SET title=?, snippet=? WHERE doc_id=?");

			for (int query_id = 200; query_id >= 1; query_id--) {
				rs = retrieve_query
						.executeQuery("SELECT A.doc_id, url, last_timestamp, title, snippet FROM khoi_search_result_detail A, khoi_search_result_cache B " 
								+ " WHERE A.doc_id = B.doc_id AND query_id = " + query_id
								+ " AND ( snippet IS NULL OR title IS NULL OR snippet = '' OR title =  '' ) "
								+ " AND query_rank >= " + offsetRecord  
								+ " AND query_rank <= " + numOfRecord
								+ " ORDER BY query_rank");
				
				String title = "";
				String snippet = "";
				System.out.println("Processing query: " + query_id);
				boolean getTitle = false, getSnippet = false;

			
				while (rs.next()) {
					
					title = rs.getString(4);
					snippet = rs.getString(5);
					url = new URL(get_url_in_archive_dot_org(rs.getString(2), rs.getString(3), replaceStr));
					getTitle = false;
					getSnippet = false;
					
					try {
						
						if (title == null){
							HTMLDocument htmlDoc = HTMLFetcher.fetch(url);
						    TextDocument doc = new BoilerpipeSAXInput(htmlDoc.toInputSource()).getTextDocument();
						    title = doc.getTitle();
						    if (title != null) getTitle = true;
						}
						else if (title.length() == 0) {
							HTMLDocument htmlDoc = HTMLFetcher.fetch(url);
						    TextDocument doc = new BoilerpipeSAXInput(htmlDoc.toInputSource()).getTextDocument();
						    title = doc.getTitle();
						    if (title != null) getTitle = true;
						}

						if (snippet == null) {
							url_conn = url.openConnection();
							url_br = new BufferedReader(new InputStreamReader(url_conn.getInputStream()));
							if (url_br != null) {
								snippet = ArticleExtractor.INSTANCE.getText(url_br)
									.replace('\t', ' ').replace('\n', ' ')
									.replace('\r', ' ');
							}
							if (snippet != null) {
								getSnippet = true;
								snippet = snippet.substring(0, (snippet.length() > 150? 150 : snippet.length()));
							}
						}
						else if (snippet.length() == 0) {
							url_conn = url.openConnection();
							url_br = new BufferedReader(new InputStreamReader(url_conn.getInputStream()));
							if (url_br != null) {
								snippet = ArticleExtractor.INSTANCE.getText(url_br)
									.replace('\t', ' ').replace('\n', ' ')
									.replace('\r', ' ');
							}
							if (snippet != null) {
								getSnippet = true;
								snippet = snippet.substring(0, (snippet.length() > 150? 150 : snippet.length()));
							}
						}
	
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					if (getTitle || getSnippet) {
						stmt.setString(1, title);
						stmt.setString(2, snippet);
						stmt.setLong(3, rs.getLong(1));
						stmt.addBatch();
					}
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