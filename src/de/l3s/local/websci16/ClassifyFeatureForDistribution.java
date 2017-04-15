package de.l3s.local.websci16;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URLDecoder;
import java.util.regex.Pattern;

public class ClassifyFeatureForDistribution {

	public static String getDest_url_archived(String dest_url) {
		String decodeURL = dest_url;
		String first = "", later = "", revfirst = "";
		int split_pos = decodeURL.indexOf('/');
		if (split_pos >= 4) {
			first = decodeURL.substring(0, split_pos);
			String[] dn = first.split(Pattern.quote("."));
			int len = dn.length;
			revfirst = dn[0];
			for (int i = 1; i < len; i++) {
				revfirst = dn[i] + "," + revfirst;
			}
			later = decodeURL.substring(split_pos + 1);
		}
		return (revfirst + ")/" + later).toLowerCase();
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

	public static String input_path = "";
	public static String output_path = "";

	public static void main(String agrs[]) {
		File directory = new File(input_path);
		// get all the files from a directory
		File[] fList = directory.listFiles();

		try {
			for (int i = 0; i < fList.length; i++) {
				BufferedReader ir = new BufferedReader(new FileReader(fList[i]));

				File outf = new File(output_path + fList[i].getName());
				BufferedWriter ow = new BufferedWriter(new FileWriter(outf));

				String rl = "";
				String[] line;
				while ((rl = ir.readLine()) != null) {
					line = rl.split("\t", 23);
					
				}
				ow.close();
				ir.close();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}