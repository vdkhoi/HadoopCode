package de.l3s.local.websci16;

import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectProcedure;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class MergeBingRankWithSample {
	static String samplePath = "D:/Alexandria/elastic_factorized_1/";
	static String bingPath = "D:/Alexandria/bing_results_domain_de_archived/";
	static String outputPath = "D:/Alexandria/elastic_factorized_bing-1/";
	static String sampleListFile = "D:/Alexandria/216_entities_list";

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

	public static String getDest_url_no_encode(String dest_url) {
		String decodeURL = dest_url;
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

	public static String PrintString(String[] parts) {
		StringBuilder sb = new StringBuilder();
		sb.append(parts[0]);
		for (int i = 1; i < parts.length; i++) {
			sb.append("\t" + parts[i]);
		}
		return sb.toString();
	}

	public static String[] getSampleEntities(String file_name) {

		try {
			File f = new File(file_name);
			BufferedReader br = new BufferedReader(new FileReader(f));
			String rl = null;
			ArrayList<String> arr = new ArrayList<String>();
			while ((rl = br.readLine()) != null) {
				arr.add(rl);
			}
			String[] strArr = new String[arr.size()];
			return arr.toArray(strArr); 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String agrs[]) {
		try {

			if (agrs.length == 4) {
				sampleListFile = agrs[0];
				samplePath = agrs[1];
				bingPath = agrs[2];
				outputPath = agrs[3];
			}

			String[] sampleEntities = getSampleEntities(sampleListFile);
			for (int i = 0; i < sampleEntities.length; i++) {
				File inf = new File(bingPath + sampleEntities[i]);
				if (!inf.exists())
					continue;
				BufferedReader ir = new BufferedReader(new FileReader(inf));

				TObjectIntHashMap<String> map = new TObjectIntHashMap<String>();

				String rl = "";
				String[] order;

				while ((rl = ir.readLine()) != null) {
					order = rl.split("\t");

					try {
						order[1] = URLDecoder.decode(order[1], "UTF-8");
					} catch (Exception e) {
						e.printStackTrace();
					}

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

					System.out.println("Process link: - " + order[1]);
					
					map.put(order[1].toLowerCase(), Integer.parseInt(order[0]));

				}
				ir.close();
				System.out.println("Process link: - " + map.size());

				// Load sample

				inf = new File(samplePath + sampleEntities[i]);
				if (!inf.exists())
					continue;
				ir = new BufferedReader(new FileReader(inf));

				File outf = new File(outputPath + sampleEntities[i]);
				if (outf.exists())
					continue;
				BufferedWriter ow = new BufferedWriter(new FileWriter(outf));

				int bingRank = 0;

				System.out.println("Elements in map - " + sampleEntities[i]
						+ ": " + map.size());

				String url = "";

				while ((rl = ir.readLine()) != null) {
					order = rl.split("\t");
					// System.out.println("Here - ");
					url = getDest_url(order[1]);
					order[1] = getDest_url_no_encode(order[1]);
					rl = PrintString(order);
					if (order[1].length() == 0)
						continue;
					if (map.size() == 0) {
						ow.write("-\t" + rl + "\r\n");
						continue;
					}
					if ((bingRank = map.remove(url)) > 0) {
						ow.write(bingRank + "\t" + rl + "\r\n");
						continue;
					}
					if ((bingRank = map.remove(url.substring(0,
							url.length() - 1))) > 0) {
						ow.write(bingRank + "\t" + rl + "\r\n");
						continue;
					}
					if ((bingRank = map.remove(url + "/")) > 0) {
						ow.write(bingRank + "\t" + rl + "\r\n");
						continue;
					}
					ow.write("-\t" + rl + "\r\n");
				}
				ow.close();
				ir.close();
				System.out.println("Exist in map - " + sampleEntities[i] + ": "
						+ map.size());

				map.forEachKey(new TObjectProcedure<String>() {
					@Override
					public boolean execute(String i) {
						System.out.println("URL: - " + i);
						return true;
					}
				});

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
