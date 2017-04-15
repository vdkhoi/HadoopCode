package de.l3s.local.websci16;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URLDecoder;
import java.util.regex.Pattern;

public class ConvertArchiveURL2WebURL {

	public static String getDest_url_archived(String dest_url ) {
    	String decodeURL = dest_url;    	
    	String first = "", later = "", revfirst = "";
    	int split_pos = decodeURL.indexOf('/');
    	if ( split_pos >= 4){
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

	public static void main(String agrs[]) {

		//String[] sampleTerm = {"Albert_Einstein", "Andreas_Nilsson", "Angela_Merkel", 
		//		"Barack_Obama", "Bruce_Willis", "Max_Planck", "Michael_Jackson", 
		//		"Wolfgang_Paul", "Scorpions", "Volkswagen", "Franz_Beckenbauer", 
		//		"Bilderbuchmuseum", "Christine_Lagarde", "Agamemnon_Schliemann", "Adolf_Hitler" };
		String path = "F:/Projects/root_set_for_hits/";
		String[] sampleTerm = {"Angela_Merkel_Root_Set", "Barack_Obama_Root_Set" };
		int URL_position = 0;

		for (int i = 0; i < sampleTerm.length; i++) {
			try {
				File inf = new File( path + sampleTerm[i]);
				BufferedReader ir = new BufferedReader(new FileReader(inf));

				File outf = new File( path + sampleTerm[i] + "_SURT");
				BufferedWriter ow = new BufferedWriter(new FileWriter(outf));

				String rl = "";
				String[] line;
				while ((rl = ir.readLine()) != null) {
					line = rl.split("\t");
					line[URL_position] = getDest_url_no_encode(line[URL_position]);
					ow.write(PrintString(line) + "\r\n");
				}
				ow.close();
				ir.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}
