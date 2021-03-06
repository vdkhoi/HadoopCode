package de.l3s.local.websci16;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashSet;

public class MergeIndividualSampleWithResultNormalFeature {

	public static String relevant_letor_file = "Z:/Alexandria/Results/Result_follow_up_WebSci16/Angela_Merkel_Manual_Label_Irrelevant.csv";
	public static String irrelevant_letor_file = "Z:/Alexandria/Results/Result_follow_up_WebSci16/Angela_Merkel_Manual_Label_Relevant.csv";
	public static String feature_path = "Z:/Alexandria/Results/Result_follow_up_WebSci16/200_entity_distribution_combined/";
	public static String entity_name = "Albert_Einstein"; 
	public static String[] feature_letor_array = { "core_pagerank",
			"domain_pagerank", "inlink", "query_freq/inlink", "search_word",
			"query_freq", "entity_type", "lucene_tf", "lucene_idf", "doc_len",
			"field_norm", "query_in_url", "revduration", "wikipedia_url",
			"url_depth", "news_url", "domain_size", "query_string", "revision",
			"anchor_time_spans" };
	
	public static String[] feature_distribution_array = {"inlink", "url_depth", "query_string", "search_word", "query_in_url", "news_url", "wikipedia_url", "core_pagerank", "domain_pagerank", "query_freq", "anchor_time_spans", "lucene_tf", "doc_len", "field_norm", "lucene_idf", "revision", "revduration", "domain_size"};

	public static String merge_letor_path = "Z:/Alexandria/Results/Result_follow_up_WebSci16/distribution_graph_chart/data_for_drawing/";

	public static HashSet<String> relevant_value = new HashSet<String>();
	public static HashSet<String> irrelevant_value = new HashSet<String>();

	public static HashSet<String> load_letor_file(String letor_file, int field_index) {
		File file = null;
		BufferedReader br = null;
		String rl = null;
		String[] fields = null;
		HashSet<String> letor = new HashSet<String>();

		try {
			file = new File(letor_file);
			br = new BufferedReader(new FileReader(file));
			while ((rl = br.readLine()) != null) {
				fields = rl.split(",");
				letor.add(fields[field_index + 2]);
			}
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return letor;
	}
	
	public static int locate_Index(String feature_name) {
		for (int i = 0; i < feature_distribution_array.length; i++) {
			if (feature_distribution_array[i].equals(feature_name)) {
				return i * 2;
			}
		}
		return -1;
	}

	public static void main(String agrs[]) {



		File load_file = null, write_file = null;
		BufferedReader bbr = null;
		BufferedWriter bbw = null;
		String rl = null;
		int distribution_index = -1;

		
		// System.out.println(entity_list[i]);
		for (int i = 0; i < feature_letor_array.length; i++) {
			
			relevant_value = load_letor_file(relevant_letor_file, i);
			irrelevant_value = load_letor_file(irrelevant_letor_file, i);
			distribution_index = locate_Index(feature_letor_array[i]);
			
			if (distribution_index < 0) continue;
			
			load_file = new File(feature_path + entity_name + ".csv");
			write_file = new File(merge_letor_path + entity_name + "_" + feature_letor_array[i]
					+ "_with_manual_label");
			try {

				bbr = new BufferedReader(new FileReader(load_file));
				bbw = new BufferedWriter(new FileWriter(write_file));
				bbr.readLine();
				while ((rl = bbr.readLine()) != null) {
					
					String[] splitted = rl.split(",");
					if (splitted.length != 36) {
						System.out.println("Error here");
						continue;
					}
					String feature_value = splitted[distribution_index];
					
					rl = splitted[distribution_index] + "\t" + splitted[distribution_index + 1]; 
					if (relevant_value
							.contains(feature_value)) {
						rl = rl + "\t" + feature_value;
						relevant_value.remove(feature_value);
					} else {
						rl = rl + "\t";
					}

					if (irrelevant_value.contains(feature_value)) {
						rl = rl + "\t" + feature_value;
						relevant_value.remove(feature_value);
					} else {
						rl = rl + "\t";
					}
					
					if (rl.equals("\t\t\t")) continue;
					
					bbw.write(rl + "\r\n");
				}
				bbr.close();
				bbw.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
