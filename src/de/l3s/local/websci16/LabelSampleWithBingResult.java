package de.l3s.local.websci16;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;


public class LabelSampleWithBingResult {

	public static String manual_letor_file = "Z:/Alexandria/Results/Result_follow_up_WebSci16/letor_folder/15_result_letor_manual";
	public static String bing_result_path = "Z:/Alexandria/Results/Result_before_WebSci16/query_results/bing_results_archived_filtered";
	public static String bing_letor_file = "Z:/Alexandria/Results/Result_follow_up_WebSci16/letor_folder/15_result_letor_bing_complement";
	
	public static String[] entity_list = new String[14];
	
	public static HashMap<String, String> sample_letor = new HashMap<String, String>(); 

	public static HashMap<String, String> load_letor_file (String letor_file) {
		File manual = null;
		BufferedReader br = null;
		String rl = null;
		String[] fields = null;
		HashMap<String, String> letor = new HashMap<String, String>();
		HashSet<String> entity = new HashSet<String>();
		try {
			manual = new File(manual_letor_file);
			br = new BufferedReader(new FileReader(manual));
			while ((rl = br.readLine()) != null) {
				fields = rl.split("#query:");
				String[] detail = fields[1].split("[|:]");
				letor.put(detail[0] + "\t" + detail[4], rl.split(" ",2)[1]);
				entity.add(detail[0]);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		entity.toArray(entity_list);
		return letor;
	}
	
	public static String getDest_url_archived(String dest_url ) {
		if (dest_url.indexOf("http://www.") == 0)
			dest_url = dest_url.replaceFirst(
					Pattern.quote("http://www."), "");

		if (dest_url.indexOf("https://www.") == 0)
			dest_url = dest_url.replaceFirst(
					Pattern.quote("https://www."), "");

		if (dest_url.indexOf("http://") == 0)
			dest_url = dest_url.replaceFirst(
					Pattern.quote("http://"), "");

		if (dest_url.indexOf("https://") == 0)
			dest_url = dest_url.replaceFirst(
					Pattern.quote("https://"), "");
		
		if (dest_url.indexOf("www.") == 0)
		dest_url = dest_url.replaceFirst(
				Pattern.quote("www."), ""); 
		
		if (dest_url.indexOf("#!") == dest_url.length() - 2)
			dest_url = dest_url.replaceFirst(
					Pattern.quote("#!"), ""); 
		
    	String decodeURL = null;
    	
    	try {
			decodeURL = URLDecoder.decode(dest_url, "UTF-8");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Error URL: - " + dest_url);
			e.printStackTrace();
		}
    	
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
        return (revfirst + ")/" + later);
    }
	
	public static void main(String agrs[]) {
		if (agrs.length == 1) {
			manual_letor_file = agrs[0];
		}
		sample_letor = load_letor_file(manual_letor_file);
		
		File bing_result = null, letor_out = null;
		BufferedReader bbr = null;
		BufferedWriter bbw = null;
		String rl = null;
		String[] fields = null;
		
		
		
		HashSet<String> letor_vector_set = new HashSet<String>();
		
		for (int i = 0; i < entity_list.length; i++) {
			//System.out.println(entity_list[i]);
			bing_result = new File (bing_result_path + "/" + entity_list[i]);
			try {
				
				bbr = new BufferedReader (new FileReader(bing_result));
				
				System.out.println("Processing: " + entity_list[i]);
				
				//if (!entity_list[i].equals("Adolf_Hitler")) continue;
				
				while ((rl = bbr.readLine()) != null) {
					
					String archive_url = getDest_url_archived(rl.split("\t")[1]);
					
					String new_letor_vector = "", old_letor_vector = "";
					
					//double reverse_rank = 1 / Double.parseDouble(rl.split("\t")[0]);
					
					double reverse_rank = 101 - Double.parseDouble(rl.split("\t")[0]);
					
					if ((old_letor_vector = sample_letor.get(entity_list[i] + "\t" + archive_url)) != null ){ 
						new_letor_vector = reverse_rank + " " + old_letor_vector;
						sample_letor.remove(entity_list[i] + "\t" + archive_url);
						//System.out.println(new_letor_vector);
					}
					else
					if ((old_letor_vector = sample_letor.get(entity_list[i] + "\t" + archive_url + "/")) != null ){ 
						//System.out.println(getDest_url_archived(rl.split("\t")[1]));
						new_letor_vector = reverse_rank + " " + old_letor_vector;
						sample_letor.remove(entity_list[i] + "\t" + archive_url + "/");
						//System.out.println(new_letor_vector);
					}
					else
					if ((old_letor_vector = sample_letor.get(entity_list[i] + "\t" + archive_url.substring(0, archive_url.length() - 1))) != null ){ 
						//System.out.println(getDest_url_archived(rl.split("\t")[1]));
						new_letor_vector = reverse_rank + " " + old_letor_vector;
						//System.out.println(new_letor_vector);
						sample_letor.remove(entity_list[i] + "\t" + archive_url.substring(0, archive_url.length() - 1));
					}
					else

					//
					if ((old_letor_vector = sample_letor.get(entity_list[i] + "\t" + archive_url.toLowerCase())) != null ){ 
						new_letor_vector = reverse_rank + " " + old_letor_vector;
						sample_letor.remove(entity_list[i] + "\t" + archive_url);
						//System.out.println(new_letor_vector);
					}
					else
					if ((old_letor_vector = sample_letor.get(entity_list[i] + "\t" + archive_url.toLowerCase() + "/")) != null ){ 
						//System.out.println(getDest_url_archived(rl.split("\t")[1]));
						new_letor_vector = reverse_rank + " " + old_letor_vector;
						sample_letor.remove(entity_list[i] + "\t" + archive_url + "/");
						//System.out.println(new_letor_vector);
					}
					else
					if ((old_letor_vector = sample_letor.get(entity_list[i] + "\t" + archive_url.toLowerCase().substring(0, archive_url.length() - 1))) != null ){ 
						//System.out.println(getDest_url_archived(rl.split("\t")[1]));
						new_letor_vector = reverse_rank + " " + old_letor_vector;
						//System.out.println(new_letor_vector);
						sample_letor.remove(entity_list[i] + "\t" + archive_url.substring(0, archive_url.length() - 1));
					}
					else {
						System.out.println(rl + "\t|\t" + entity_list[i] + "\t" + archive_url.toLowerCase() + "\t|\t" + sample_letor.get(entity_list[i] + "\t" + archive_url.toLowerCase()));
					}
					
					if (!new_letor_vector.equals("")) {
						letor_vector_set.add(new_letor_vector);
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			bbr.close();
			
			for (String rest_letor: sample_letor.keySet() ) {
				letor_vector_set.add("0.0 " + sample_letor.get(rest_letor));
			}
			letor_out = new File(bing_letor_file);
			bbw = new BufferedWriter (new FileWriter(letor_out));
			
			for (String letor:  letor_vector_set) {
				bbw.write(letor + "\r\n");
			}

			
			bbw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
