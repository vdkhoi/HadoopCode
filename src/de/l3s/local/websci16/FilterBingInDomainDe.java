package de.l3s.local.websci16;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.regex.Pattern;

public class FilterBingInDomainDe {

	public static void main(String agrs[]) {
		try {

			File f = new File(
					"Z:/Alexandria/Data_Input/fix_entity.txt");
			BufferedReader br = new BufferedReader(new FileReader(f));
			String rl = null;
			while ((rl = br.readLine()) != null) {
				String[] order = rl.split("\t", 2);
				File o = new File(
						"Z:/Alexandria/Results/query_results/bing_results_archived/"
								+ order[1]);
				if (!o.exists())
					continue;
				BufferedReader or = new BufferedReader(new FileReader(o));

				File of = new File(
						"Z:/Alexandria/Results/query_results/bing_results_domain_de/"
								+ order[1]);

				BufferedWriter ofr = new BufferedWriter(new FileWriter(of));

				TObjectIntHashMap<String> map = new TObjectIntHashMap<String>();

				while ((rl = or.readLine()) != null) {
					order = rl.split("\t");

					if (order[1].indexOf("http://www.") == 0)
						order[1] = order[1].replaceFirst(
								Pattern.quote("http://www."), "");

					if (order[1].indexOf("https://www.") == 0)
						order[1] = order[1].replaceFirst(
								Pattern.quote("https://www."), "");

					if (order[1].indexOf("http://") == 0)
						order[1] = order[1].replaceFirst(
								Pattern.quote("http://"), "");

					if (order[1].indexOf("https://") == 0)
						order[1] = order[1].replaceFirst(
								Pattern.quote("https://"), "");

					if (order[1].substring(order[1].indexOf('/') - 2,
							order[1].indexOf('/')).equals("de")) {
						if (!map.containsKey(order[1])) {
							System.out.println("Process link: - " + order[1]);
							map.put(order[1], Integer.parseInt(order[0]));
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
