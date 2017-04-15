package de.l3s.local.websci16;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.regex.Pattern;

public class FilterBingInArchive {

	public static void main(String agrs[]) {
		try {

			File f = new File(
					"Z:/Alexandria/Data_Input/sample_entity_ordered.txt");
			BufferedReader br = new BufferedReader(new FileReader(f));
			String rl = null;
			while ((rl = br.readLine()) != null) {
				String[] order = rl.split("\t");
				File o = new File(
						"Z:/Alexandria/Results/query_results/bing_results_domain_de/"
								+ order[1]);
				
				if (!o.exists()) continue;
				
				BufferedReader or = new BufferedReader(new FileReader(o));
				
				

				File of = new File(
						"Z:/Alexandria/Results/query_results/bing_results_domain_de_archived/"
								+ order[1]);
				BufferedWriter ofr = new BufferedWriter(new FileWriter(of));

				while ((rl = or.readLine()) != null) {
					order = rl.split("\t");
					System.out.println("Process link: - " + order[1] + " ...");
					if (order[3].compareTo("null") != 0) {
						if (order[3].compareTo("2014-01-01 00:00:00.0") < 0) {
							ofr.write(rl + "\r\n");
						}
					}
				}

				or.close();
				ofr.close();
			}
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
