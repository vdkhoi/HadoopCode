package de.l3s.local.websci16;

import gnu.trove.map.hash.TIntIntHashMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;

public class InsertQueryColumnIntoSearchResult {
	public static String RESULT_SEARCH_PATH = "C:/Users/Khoi/Dropbox/Alexandria Research/sample_url/";
	public static String QUERY_FREQ_PATH = "Z:/Alexandria/Results/sample_results_with_query_frequency/";
	public static String MERGE_RESULT_PATH = "Z:/Alexandria/Results/sample_results_from_Tuan/";

	public static String merge_query_freq(int freq, String[] line) {
		String result = "";
		for(int i = 0; i < 3; i++) {
			result += line[i] + "\t";
		}
		result += freq;
		for(int i = 3; i < line.length; i++) {
			result += "\t" + line[i] ;
		}
		return result;
	}

	public static String getDest_url_no_encode(String dest_url ) {
    	String decodeURL = dest_url;    	
    	String first = "", later = "", revfirst = "";
    	int split_pos = decodeURL.indexOf(')');
    	if ( split_pos >= 4){
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
	
	public static void main(String agrs[]) {
		String[] sampleEntities= {"Albert_Einstein", "Andreas_Nilsson", "Angela_Merkel",
				"Barack_Obama", "Bruce_Willis", "Max_Planck", "Michael_Jackson",
				"Wolfgang_Paul", "Scorpions", "Volkswagen"};
		
		//String[] sampleEntities= {"Scorpions"};

		
		String rl = "";
		String[] line;
		
		File rf, qf, mf;
		BufferedReader rr, qr;
		BufferedWriter mw;
		
		for (int i = 0; i < sampleEntities.length; i ++) {
			try {
				
				
				qf = new File(RESULT_SEARCH_PATH + sampleEntities[i] + ".allscore");
				qr = new BufferedReader(new FileReader(qf));
				
				HashMap<String,String> query_freq_map = new HashMap<String,String>();				
				while ((rl = qr.readLine()) != null) {
					line = rl.split("\t");
					if (line.length < 3) continue;
					query_freq_map.put(line[0], rl);
				}
				qr.close();
							
				rf = new File(QUERY_FREQ_PATH + sampleEntities[i]);
				rr = new BufferedReader(new FileReader(rf));
				
				mf = new File(MERGE_RESULT_PATH + sampleEntities[i] + ".allscore");
				mw = new BufferedWriter(new FileWriter(mf));
				
				while ((rl = rr.readLine()) != null) {
					line = rl.split("\t");
					String result = query_freq_map.remove(getDest_url_no_encode(line[1]));
					if (result != null) {
						mw.write(merge_query_freq(Integer.parseInt(line[2]), result.split("\t")) + "\r\n");
					}
				}
				
				rr.close();
				mw.close();
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			
			
		}
		
	}
}
