package de.l3s.hadoop.mapreduce.graph.hits.prepare;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URLDecoder;
import java.util.regex.Pattern;

//Prepare datasets:
//step1: Fetch root set from Bing 
//step2: Convert Bing result to SURT form  (this program)
//step3: Build base set from map/reduce 

public class ConvertWebURL2ArchiveURL {

	public static String toSURTMassageURL(String dest_url ) {
    	dest_url = dest_url
    					.replaceFirst(Pattern.quote("https://www."), "")
    					.replaceFirst(Pattern.quote("http://www."), "")
    					.replaceFirst(Pattern.quote("https://"), "")
    					.replaceFirst(Pattern.quote("http://"), "");
    					
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

	public static String PrintString(String[] parts) {
		StringBuilder sb = new StringBuilder();
		sb.append(parts[0]);
		for (int i = 1; i < parts.length; i++) {
			sb.append("\t" + parts[i]);
		}
		return sb.toString();
	}

	public static void main(String agrs[]) {
		String[] sampleTerm = {"Barack_Obama"};

		for (int i = 0; i < sampleTerm.length; i++) {
			try {
				File inf = new File(
						"Z:/Alexandria/Results/Result_follow_up_WebSci16/Live_Bing_Result_Fetching/"
								+ sampleTerm[i]);
				BufferedReader ir = new BufferedReader(new FileReader(inf));

				File outf = new File(
						"Z:/Alexandria/Results/Result_follow_up_WebSci16/Live_Bing_Result_Fetching/"
								+ sampleTerm[i] + "_SURT");
				BufferedWriter ow = new BufferedWriter(new FileWriter(outf));

				String rl = "";
				String[] line;
				while ((rl = ir.readLine()) != null) {
					line = rl.split("\t");
					line[0] = toSURTMassageURL(line[0]);
					if (line[0].substring(0, 2).equals("de"))
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
