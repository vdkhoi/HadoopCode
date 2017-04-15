package de.l3s.hadoop.mapreduce.graph.hits.prepare;


import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.regex.Pattern;

// There are two steps to compute HITS scores  run on local
// step1: convert graph by integer id, split to subgraph by year (this program)
// step2: calculate score of each subgraph
// Input: graph by string url
// Format src_url TAB time TAB dest_url


public class ComputeRootSetFromBaseSet {

	public static final String ENTITY		 		= "Barack_Obama";
	
	
	public static String convert2SURTForm(String dest_url) {
		
		String decodeURL = dest_url;
		
		if (decodeURL.indexOf("http://www.") == 0)
			decodeURL = decodeURL.replaceFirst(
					Pattern.quote("http://www."), "");

		if (decodeURL.indexOf("https://www.") == 0)
			decodeURL = decodeURL.replaceFirst(
					Pattern.quote("https://www."), "");

		if (decodeURL.indexOf("http://") == 0)
			decodeURL = decodeURL.replaceFirst(
					Pattern.quote("http://"), "");

		if (decodeURL.indexOf("https://") == 0)
			decodeURL = decodeURL.replaceFirst(
					Pattern.quote("https://"), "");
		
		if (decodeURL.indexOf("www.") == 0)
			decodeURL = decodeURL.replaceFirst(
				Pattern.quote("www."), "");
		
    	String revfirst = "";
    	
    	
		String[] dn = decodeURL.split(Pattern.quote("."));
		int len = dn.length;
		revfirst = dn[0];
		for (int i = 1; i < len; i++) {
			revfirst = dn[i] + "," + revfirst;
		}
        return (revfirst + ")/").toLowerCase();
    }

	
	
	public static void main(String agrs[]) {
		
		try {
			File base_set_inf = new File(
					"Z:/Alexandria/Results/Result_follow_up_WebSci16/base_set_for_hits/" + ENTITY + "_Base_Set");
			BufferedReader ir = new BufferedReader(new FileReader(base_set_inf));

			File bing_result_inf = new File(
					"Z:/Alexandria/Results/Result_follow_up_WebSci16/Live_Bing_Result_Fetching/" + ENTITY + "_SURT");
			BufferedReader br = new BufferedReader(new FileReader(bing_result_inf));
			
			File root_set_outf = new File(
					"Z:/Alexandria/Results/Result_follow_up_WebSci16/root_set_for_hits/" + ENTITY + "_Root_Set");
			BufferedWriter rw = new BufferedWriter(new FileWriter(root_set_outf));
					
			
			TObjectIntHashMap<String> bing_result_set = new TObjectIntHashMap<String>();
			
			String rl = "";
			
			int rank = 0;
			
			while ((rl = br.readLine()) != null) {
				bing_result_set.put(rl, rank ++);				
			}
			
			br.close();
			String line[] = null;
			
			while (((rl = ir.readLine()) != null) && (!bing_result_set.isEmpty())) {
				line = rl.split("\t");
				if (bing_result_set.containsKey(line[0])){
					if (line[0].equals("de,angela-merkel)/")) 
						System.out.println("Here");
					rw.write(line[0] + "\t" + bing_result_set.get(line[0]) + "\r\n");
					bing_result_set.remove(line[0]);
				}
				if (bing_result_set.containsKey(line[2])){
					rw.write(line[2] + "\t" + bing_result_set.get(line[2]) + "\r\n");
					bing_result_set.remove(line[2]);
				}
			}
			
			rw.close();
			ir.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
