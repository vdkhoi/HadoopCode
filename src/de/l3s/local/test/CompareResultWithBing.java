package de.l3s.local.test;

import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;
import java.util.regex.Pattern;

/**
 *
 */
public class CompareResultWithBing {

	public static String query_file = "Z:/Alexandria/Data_Input/sample_entity_for_analysis";

	public static String overlap_path = "Z:/Alexandria/Results/evaluation_task1_1/";

	public static String elastic_path = "Z:/Alexandria/Results/all_entity.txt";

	public static String bing_path = "Z:/Alexandria/Results/query_results/bing_results_domain_de_archived/";
	
	public static String sample_entity_for_analysis = "Z:/Alexandria/Results/sample_entity_for_analysis/";

	public static void main(String[] args) throws Exception {

		if (args.length == 4) {
			query_file = args[0];
			overlap_path = args[1];
			bing_path = args[2];
			elastic_path = args[3];
		}

		String rl = "";
		String[] line = new String[2];
		File qf = new File(query_file);
		BufferedReader br = new BufferedReader(new FileReader(qf));
		while ((rl = br.readLine()) != null) {

			line = rl.split("\t");

			File rf = new File(overlap_path + line[1]);
			if (rf.exists())
				continue;

			File ef = new File(elastic_path);
			if (!ef.exists())
				continue;

			File bf = new File(bing_path + line[1]);
			if (!bf.exists())
				continue;

			BufferedReader bbr = new BufferedReader(new FileReader(bf));
			String result_line = "";
			TObjectIntHashMap<String> bing_resultlist = new TObjectIntHashMap<String>();
			while ((result_line = bbr.readLine()) != null) {

				String[] b_line = result_line.split("\t");
				int query_freq = 0;
				try {
					query_freq = Integer.parseInt(b_line[0]);
				}
				catch (NumberFormatException e) {
					continue;
				}

				if (b_line[1].indexOf("http://www.") == 0)
					b_line[1] = b_line[1].replaceFirst(
							Pattern.quote("http://www."), "");

				if (b_line[1].indexOf("https://www.") == 0)
					b_line[1] = b_line[1].replaceFirst(
							Pattern.quote("https://www."), "");

				if (b_line[1].indexOf("http://") == 0)
					b_line[1] = b_line[1].replaceFirst(
							Pattern.quote("http://"), "");

				if (b_line[1].indexOf("https://") == 0)
					b_line[1] = b_line[1].replaceFirst(
							Pattern.quote("https://"), "");

				
				
				bing_resultlist.put(b_line[1], query_freq);
				// System.out.println(b_line[1]);
			}
			bbr.close();
			
			int bingRank;
			
			bbr = new BufferedReader(new FileReader(ef));
			BufferedWriter bw = new BufferedWriter(new FileWriter(rf));
			//while (((result_line = bbr.readLine()) != null)
			//		&& (!bing_resultlist.isEmpty())) {
				
				
			while ((result_line = bbr.readLine()) != null) {
				String[] features = result_line.split("\t");
				
				/*
				if ((bingRank = bing_resultlist.remove(features[51])) > 0) {
					bw.write(features[51] + "\t" + features[6] + "\t" + features[7] + "\t" + features[8] + "\t" + bingRank + "\r\n");
					continue;
				}
				if ((bingRank = bing_resultlist.remove(features[51].substring(0, features[51].length() - 1))) > 0) {
					bw.write(features[51] + "\t" + features[6] + "\t" + features[7] + "\t" + features[8] + "\t" + bingRank + "\r\n");
					continue;
				}
				if ((bingRank = bing_resultlist.remove(features[51] + "/")) > 0) {
					bw.write(features[51] + "\t" + features[6] + "\t" + features[7] + "\t" + features[8] + "\t" + bingRank + "\r\n");
					continue;
				}
				*/
				
				if (features[0].equals("1")){
					bw.write(features[51] + "\t" + features[6] + "\t" + features[7] + "\t" + features[8] + "\r\n");
				}
				
			}
			bbr.close();
			bw.close();
		}
	}
}