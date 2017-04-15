package de.l3s.local.websci16;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class CalculateFeatureDistributionForEntity {

	public static String root_path = "Z:/Alexandria/Results/Result_follow_up_WebSci16/200_entity_distribution_separately";

	public static String entity_file = "Z:/Alexandria/Results/Result_follow_up_WebSci16/6_entities_list";

	public static String output_path = "Z:/Alexandria/Results/Result_follow_up_WebSci16/200_entity_distribution_combined/";

	public static String[] feature_list = { "inlink", "url_depth",
			"query_string", "search_word", "query_in_url", "news_url",
			"wikipedia_url", "core_pagerank", "domain_pagerank", "query_freq",
			"anchor_time_spans", "lucene_tf", "doc_len", "field_norm",
			"lucene_idf", "revision", "revduration", "domain_size" }; // Missing
																		// entity_type

	public static ArrayList<String> load_entity() {
		ArrayList<String> ent_list = new ArrayList<String>();
		try {
			File ent_file = new File(entity_file);
			BufferedReader buf_read = new BufferedReader(new FileReader(
					ent_file));
			String rl = "";
			while ((rl = buf_read.readLine()) != null) {
				ent_list.add(rl.split("\t")[1]);
			}
			buf_read.close();
			return ent_list;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ent_list;
	}



	public static String PrintString(String[] parts) {
		StringBuilder sb = new StringBuilder();
		sb.append(parts[0]);
		for (int i = 1; i < parts.length; i++) {
			sb.append("\t" + parts[i]);
		}
		return sb.toString();
	}

	public static void main(String agrs[]) {
		ArrayList<String> ent_list = load_entity();

		File feature_dir = null;
		File feature_file = null;
		BufferedReader[] feature_readers = new BufferedReader[feature_list.length];
		StringBuilder out_str = new StringBuilder();

		// Khoi tao buffer cho tung feature

		for (int i = 0; i < ent_list.size(); i++) {
			for (int j = 0; j < feature_list.length; j++) {

				try {
					feature_file = new File(root_path + "/" + feature_list[j]
							+ "/" + ent_list.get(i) + "/part-r-00000");
					feature_readers[j] = new BufferedReader(new FileReader(
							feature_file));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

			StringBuilder out_content = new StringBuilder();
			String rl = "";
			boolean[] feature_stop = new boolean[feature_list.length];
			boolean all_stop = false;
			
			
			out_content.delete(0, out_content.capacity());
			
			for (int j = 0; j < feature_list.length; j++) {
				feature_stop[j] = false;
				out_content.append("," + feature_list[j] + ((j == feature_list.length - 1)? "\r\n" : ","));
			}
			
			System.out.println(out_content.toString());
			while (!all_stop) {
				for (int j = 0; j < feature_list.length; j++) {
					try {

						
						if ((rl = feature_readers[j].readLine()) != null) {
							out_content.append(rl.split("\t")[0] + "," + rl.split("\t")[1] + ((j == feature_list.length - 1)? "\r\n" : ","));
						} else {
							out_content.append("," + ((j == feature_list.length - 1)? "\r\n" : ","));
							feature_stop[j] = true;
						}

					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				all_stop = feature_stop[0];
				for (int j = 1; j < feature_list.length; j++) {
					all_stop = all_stop && feature_stop[j];
				}
			}
			try {
				File outf = new File(output_path + "/" + ent_list.get(i)
						+ ".csv");
				BufferedWriter outw;
				for (int j = 0; j < feature_list.length; j++) {
					feature_readers[j].close();
				}

				outw = new BufferedWriter(new FileWriter(outf));
				outw.write(out_content.toString());
				outw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
