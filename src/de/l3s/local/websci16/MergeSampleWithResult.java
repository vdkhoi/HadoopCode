package de.l3s.local.websci16;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashSet;

public class MergeSampleWithResult {

	public static String relevant_letor_file = "Z:/Alexandria/Results/Result_follow_up_WebSci16/letor_folder/relevant_sample.csv";
	public static String irrelevant_letor_file = "Z:/Alexandria/Results/Result_follow_up_WebSci16/letor_folder/irrelevant_sample.csv";
	public static String feature_path = "Z:/Alexandria/Results/Result_follow_up_WebSci16/200_entity_distribution/";
	public static String[] feature_file_array = { "core_pagerank",
			"domain_pagerank", "inlink", "query_freq/inlink", "search_word",
			"query_freq", "entity_type", "lucene_tf", "lucene_idf", "doc_len",
			"field_norm", "query_in_url", "revduration", "wikipedia_url",
			"url_depth", "news_url", "domain_size", "query_string", "revision",
			"anchor_time_spans" };

	public static String merge_letor_file = "Z:/Alexandria/Results/Result_follow_up_WebSci16/letor_folder/";

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

	public static void main(String agrs[]) {



		File load_file = null, write_file = null;
		BufferedReader bbr = null;
		BufferedWriter bbw = null;
		String rl = null;


		
		// System.out.println(entity_list[i]);
		for (int i = 0; i < feature_file_array.length; i++) {
			
			relevant_value = load_letor_file(relevant_letor_file, i);
			irrelevant_value = load_letor_file(irrelevant_letor_file, i);
			
			load_file = new File(feature_path + feature_file_array[i] + "/"
					+ feature_file_array[i]);
			write_file = new File(merge_letor_file + feature_file_array[i]
					+ "_with_manual_label");
			try {

				bbr = new BufferedReader(new FileReader(load_file));
				bbw = new BufferedWriter(new FileWriter(write_file));

				while ((rl = bbr.readLine()) != null) {
					String feature_value = null;
					if (relevant_value
							.contains(feature_value = rl.split("\t")[0])) {
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
