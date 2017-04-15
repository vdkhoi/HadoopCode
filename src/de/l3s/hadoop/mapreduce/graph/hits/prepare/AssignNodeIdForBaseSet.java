package de.l3s.hadoop.mapreduce.graph.hits.prepare;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;

// There are two steps to compute HITS scores  run on local
// step1: convert graph by integer id, split to subgraph by year (this program)
// step2: calculate score of each subgraph
// Input: graph by string url
// Format src_url TAB time TAB dest_url


public class AssignNodeIdForBaseSet {

	public static final String ENTITY		 		= "Angela_Merkel";
	
	static class PQsort implements Comparator<String> {
		 

		@Override
		public int compare(String o1, String o2) {
			return o1.compareTo(o2);
		}
	}
	
	static String host1, host2;
	
	public static boolean isSameHost(String url1, String url2){
		int index1 = url1.indexOf(")/"), index2 = url2.indexOf(")/");
		host1 = url1.substring(0, index1);
		host2 = url2.substring(0, index2);
		if (host1.equals(host2)) return true;
		return false;
	}
	
	public static boolean isSameDomain(String url1, String url2){
		String[] parts1 = host1.split(",");
		String[] parts2 = host2.split(",");

		if (parts1[0].equals(parts2[0]) && parts1[1].equals(parts2[1])) return true;
		
		return false;
	}
	
	
	
	public static void main(String agrs[]) {
		

	
		try {
			File inf = new File(
					"Z:/Alexandria/Results/Result_follow_up_WebSci16/base_set_for_hits/" + ENTITY + "_Base_Set");
			BufferedReader ir = new BufferedReader(new FileReader(inf));

			File outf = new File(
					"Z:/Alexandria/Results/Result_follow_up_WebSci16/base_set_for_hits/" + ENTITY + "_Base_Set_By_Id");
			BufferedWriter ow = new BufferedWriter(new FileWriter(outf));
			

			PQsort pqs = new PQsort();
			PriorityQueue<String> queue_url = new PriorityQueue<String> (10, pqs);
			
			HashSet<String> unique = new HashSet<String>();
			HashSet<String> year_set = new HashSet<String>();
			
			String rl = "";
			String[] line;
			while ((rl = ir.readLine()) != null) {
				line = rl.split("\t");
				if (!unique.contains(line[0])) {
					unique.add(line[0]);
					queue_url.add(line[0]);
				}
				
				if (!unique.contains(line[2])) {
					unique.add(line[2]);
					queue_url.add(line[2]);
				}
				
				year_set.add(line[1].substring(0, 4));
			}
			
			ArrayList<String> year = new ArrayList<String>(year_set); 
			
			int id = 0;
			TObjectIntHashMap<String> map_id_url = new TObjectIntHashMap<String>();
			while ((rl = queue_url.poll()) != null) {
				ow.write(id + "\t" + rl + "\r\n");
				map_id_url.put(rl, id);
				id++;
			}
			ir.close();
			ow.close();
			
			int num_of_year = year.size();
			
			File[] outf_id_map = new File[num_of_year];
			File[] outf_id_map_cross_host = new File[num_of_year];
			File[] outf_id_map_cross_domain = new File[num_of_year];
			BufferedWriter[] ow_id_map = new BufferedWriter[num_of_year];
			BufferedWriter[] ow_id_map_cross_host = new BufferedWriter[num_of_year];
			BufferedWriter[] ow_id_map_cross_domain = new BufferedWriter[num_of_year];
			
			for(int y = 0; y < num_of_year; y++ ){
				outf_id_map[y] = new File(
					"Z:/Alexandria/Results/Result_follow_up_WebSci16/base_set_for_hits/" + ENTITY + "_Base_Set_Links_By_Id_" + year.get(y));
				ow_id_map[y] = new BufferedWriter(new FileWriter(outf_id_map[y]));
			
				outf_id_map_cross_host[y] = new File(
					"Z:/Alexandria/Results/Result_follow_up_WebSci16/base_set_for_hits/" + ENTITY + "_Base_Set_Links_Cross_Host_By_Id_" + year.get(y));
				ow_id_map_cross_host[y] = new BufferedWriter(new FileWriter(outf_id_map_cross_host[y]));
			
				outf_id_map_cross_domain[y] = new File(
					"Z:/Alexandria/Results/Result_follow_up_WebSci16/base_set_for_hits/" + ENTITY + "_Base_Set_Links_Cross_Domain_By_Id_" + year.get(y));
				ow_id_map_cross_domain[y] = new BufferedWriter(new FileWriter(outf_id_map_cross_domain[y]));
			}
			
			ir = new BufferedReader(new FileReader(inf));
			while ((rl = ir.readLine()) != null) {
				line = rl.split("\t");
				int id1 = map_id_url.get(line[0]);
				int id2 = map_id_url.get(line[2]);
				String str_year = line[1].substring(0, 4);
				int writing_year = year.indexOf(str_year);
				ow_id_map[writing_year].write("" + id1 + "\t" + line[1] + "\t" + id2 + "\r\n");
				if (id1 != id2){
					if (!isSameHost(line[0], line[2])) {
						ow_id_map_cross_host[writing_year].write("" + id1 + "\t" + line[1] + "\t" + id2 + "\r\n");
						if (!isSameDomain(line[0], line[1])) {
							ow_id_map_cross_domain[writing_year].write("" + id1 + "\t" + line[1] + "\t" + id2 + "\r\n");
						}
					}
				}
					
			}

			ir.close();
			for(int y = 0; y < num_of_year; y++ ){
				ow_id_map[y].close();
				ow_id_map_cross_host[y].close();
				ow_id_map_cross_domain[y].close();
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}
}
