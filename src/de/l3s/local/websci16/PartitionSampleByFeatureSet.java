package de.l3s.local.websci16;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.stringtemplate.v4.compiler.STParser.arg_return;


public class PartitionSampleByFeatureSet {
	public static String input_letor_file = "Z:/Alexandria/Results/Result_follow_up_WebSci16/letor_folder/15_result_letor_manual";
	public static String feature_to_remove_file = "4,6,12";
	public static String output_letor_file = "Z:/Alexandria/Results/Result_follow_up_WebSci16/letor_folder/15_result_letor_manual_removed_4_6_12";
	public static void main (String agrs[]){
		try {
			
			if (agrs.length == 3) {
				input_letor_file = agrs[0];
				output_letor_file = agrs[1];
				feature_to_remove_file = agrs[2];
			}
			
			HashSet<Integer> set_of_remove_feature = new HashSet<Integer>();
			String[] feature_array = feature_to_remove_file.split(",");
			for (int i = 0; i < feature_array.length; i ++) {
				set_of_remove_feature.add(Integer.parseInt(feature_array[i]));
			}
			
			System.out.println("Processing file: " + input_letor_file + "...");
			
			File f = new File(input_letor_file), o = new File(output_letor_file);
			BufferedReader br = new BufferedReader(new FileReader(f));
			BufferedWriter wr = new BufferedWriter(new FileWriter(o));
			int i = 0;
			String curr_line = "", new_letor = "";
			String[] letor_parts;
			while ((curr_line = br.readLine()) != null){
				letor_parts = curr_line.split(" ");
				new_letor = letor_parts[0] + " " + letor_parts[1];
				int current_feature_index = 1;
				for (int k = 2; k < letor_parts.length; k++){
					String[] feature_value = letor_parts[k].split(":");
					if (feature_value.length == 1) {
						letor_parts[k] += ("0");
						feature_value = letor_parts[k].split(":");
					}
					if (feature_value[0].matches("^\\d+$")) {
						if (! set_of_remove_feature.contains(Integer.parseInt(feature_value[0]))) {
							new_letor += (" " + current_feature_index + ":" + feature_value[1]);
							current_feature_index ++;
						}
					}
					else {
						new_letor += (" " + letor_parts[k]);
					}
				}
				wr.write(new_letor + "\r\n");
			}
			
			br.close();
			wr.close();
			System.out.println("finished");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
