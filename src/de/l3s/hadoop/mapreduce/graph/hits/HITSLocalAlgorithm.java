package de.l3s.hadoop.mapreduce.graph.hits;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntProcedure;
import gnu.trove.procedure.TObjectProcedure;
import gnu.trove.set.hash.TIntHashSet;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Comparator;
import java.util.PriorityQueue;

//There are two steps to compute HITS scores  run on local
//step1: convert graph by integer id, split to subgraph by year 
//step2: calculate score of each subgraph  (this program)
//Input: graph by string url
//Format src_url TAB time TAB dest_url



class HITSNode{
	public int node_id;
	public float a_score = 1.0f;
	public float h_score = 1.0f;
	
	
	public HITSNode(String node){
		node_id= Integer.parseInt(node);
		a_score = 1.0f;
		h_score = 1.0f;
	}
	
	public HITSNode(int node){
		node_id = node;
		a_score = 1.0f;
		h_score = 1.0f;
	}
	
	
}

class PQSort implements Comparator<HITSNode> {
	 
	public byte print_mode = 0; // 0: auth score desc; 1: hub score desc

	@Override
	public int compare(HITSNode o1, HITSNode o2) {
		if (print_mode == 0)
			return (o1.a_score < o2.a_score)? 1 : ((o1.a_score == o2.a_score)? 0 : -1);
		else
			return (o1.h_score < o2.h_score)? 1 : ((o1.h_score == o2.h_score)? 0 : -1);
		
	}}

public class HITSLocalAlgorithm {

	public static final String ENTITY		 		= "Barack_Obama";  //Change here
	public static final String SUB_GRAPH	 		= "Cross_Host";  //Change here
	public static final String LINK_GRAPH_FILE 		= "Z:/Alexandria/Results/Result_follow_up_WebSci16/base_set_for_hits/" + ENTITY + "_Base_Set_Links_" + SUB_GRAPH + "_By_Id_2013";
	public static final String NODE_ID_MAPPING_FILE = "Z:/Alexandria/Results/Result_follow_up_WebSci16/base_set_for_hits/" + ENTITY + "_Base_Set_By_Id";
	public static final int    NUM_OF_ITERATION 	= 10;
	static TIntObjectHashMap<TIntHashSet> adjacent_out_graph = new TIntObjectHashMap<TIntHashSet>();
	static TIntObjectHashMap<TIntHashSet> adjacent_in_graph = new TIntObjectHashMap<TIntHashSet>();
	
	static TIntObjectHashMap<HITSNode> object_in_map = new TIntObjectHashMap<HITSNode>();
	static TIntObjectHashMap<HITSNode> object_out_map = new TIntObjectHashMap<HITSNode>(); 
	static TIntObjectHashMap<HITSNode> object_map = new TIntObjectHashMap<HITSNode>();
	
	static boolean exist(HITSNode node, TIntObjectHashMap<HITSNode> object_map) {
		if (object_map.contains(node.node_id))
			return true;
		
		object_map.put(node.node_id, node);
		return false;
	}
	
	// format of input
	// source \t destination
	static void load_graph(String file_name) {
		File input_file = new File (file_name);
		String rl = "";
		String[] nodes;
		try {
			BufferedReader buf_in = new BufferedReader(new FileReader(input_file));
			while ((rl = buf_in.readLine()) != null) {
				nodes = rl.split("\t");
				HITSNode node0 = new HITSNode(nodes[0]);
				HITSNode node1 = new HITSNode(nodes[2]);
				
				if (!exist(node0, object_out_map)) {
					TIntHashSet adjacent_nodes = new TIntHashSet();
					adjacent_nodes.add(node1.node_id);
					adjacent_out_graph.put(node0.node_id, adjacent_nodes);
				}
				else
				{
					TIntHashSet adjacent_nodes = adjacent_out_graph.get(node0.node_id);
					adjacent_nodes.add(node1.node_id);
					adjacent_out_graph.put(node0.node_id, adjacent_nodes);
				}
				
				
				if (!exist(node1, object_in_map)) {
					TIntHashSet adjacent_nodes = new TIntHashSet();
					adjacent_nodes.add(node0.node_id);
					adjacent_in_graph.put(node1.node_id, adjacent_nodes);
				}
				else
				{
					TIntHashSet adjacent_nodes = adjacent_in_graph.get(node1.node_id);
					adjacent_nodes.add(node0.node_id);
					adjacent_in_graph.put(node1.node_id, adjacent_nodes);
				}
			}
			
			object_in_map.putAll(object_out_map);  // check if operation is copied or duplicate memory
			object_map = object_in_map;
			
			
			buf_in.close();
			
		} catch (Exception e) {
			System.out.println(rl);
			e.printStackTrace();
		}
		
		
	}
	
	static float norm = 0.0f;
	
	static HITSNode el;
	
	static void HITSCompute() {
		
		for (int interator = 0; interator < NUM_OF_ITERATION; interator ++) {
			norm = 0.0f;
			object_map.forEachKey(new TIntProcedure() {

				@Override
				public boolean execute(int node_id) {
					TIntHashSet adjacent_nodes = adjacent_in_graph.get(node_id);
					if (adjacent_nodes == null) return true;
					
					el = object_map.get(node_id);
					el.a_score = 0.0f;
									
					adjacent_nodes.forEach(new TIntProcedure() {

						@Override
						public boolean execute(int adjacent_node) {
							el.a_score += object_map.get(adjacent_node).h_score;
							return true;
						}
						
					});

					norm += Math.sqrt(el.a_score);
					

					return true;
				}
				
			});
			
			object_map.forEachKey(new TIntProcedure() {

				@Override
				public boolean execute(int node_id) {
					HITSNode e = object_map.get(node_id);
					e.a_score /= norm;
					return true;
				}
				
			});
			
			norm = 0.0f;
			
			object_map.forEachKey(new TIntProcedure() {

				@Override
				public boolean execute(int node_id) {
					
					TIntHashSet adjacent_nodes = adjacent_out_graph.get(node_id);
					
					if (adjacent_nodes == null) return true;
					
					el = object_map.get(node_id);
					el.h_score = 0.0f;
					
					adjacent_nodes.forEach(new TIntProcedure() {

						@Override
						public boolean execute(int adjacent_node) {
							el.h_score += object_map.get(adjacent_node).a_score;
							return true;
						}
						
					});
					norm += Math.sqrt(el.h_score);

					return true;
				}
				
			});
			
			object_map.forEachKey(new TIntProcedure() {

				@Override
				public boolean execute(int node_id) {
					HITSNode e = object_map.get(node_id);
					e.h_score /= norm;
					return true;
				}
				
			});
			
		}
	}
	
	static TIntObjectHashMap<String> node_id_mapping = new TIntObjectHashMap<String>(); 
	
	static void load_node_id_mapping(String file_name) {
		File input_file = new File (file_name);
		String rl = "";
		String[] nodes;
		try {
			BufferedReader buf_in = new BufferedReader(new FileReader(input_file));
			while ((rl = buf_in.readLine()) != null) {
				nodes = rl.split("\t");
				node_id_mapping.put(Integer.parseInt(nodes[0]), nodes[1]);
			}
			
			buf_in.close();
			
		} catch (Exception e) {
			System.out.println(rl);
			e.printStackTrace();
		}
	}
	
	static void printScore() {
		
		load_node_id_mapping(NODE_ID_MAPPING_FILE);
		
		PQSort sorter = new PQSort();
		PriorityQueue<HITSNode> order = new PriorityQueue<HITSNode>(sorter);
		order.addAll(object_map.valueCollection());
		HITSNode el;
		File outp = new File(LINK_GRAPH_FILE + "_Auth_Score_DESC");
		BufferedWriter outw;
		try {
			outw = new BufferedWriter(new FileWriter(outp));
			while ((el = order.poll()) != null) {
				outw.write(node_id_mapping.get(el.node_id) + "\t" + el.a_score + "\t" + el.h_score + "\r\n");
			}
			outw.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		sorter.print_mode = 1;
		order = new PriorityQueue<HITSNode>(sorter);
		order.addAll(object_map.valueCollection());
		outp = new File(LINK_GRAPH_FILE + "_Hub_Score_DESC");
		
		try {
			outw = new BufferedWriter(new FileWriter(outp));
			while ((el = order.poll()) != null) {
				outw.write(node_id_mapping.get(el.node_id) + "\t" + el.a_score + "\t" + el.h_score + "\r\n");
			}
			outw.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		
//		object_map.forEachValue(new TObjectProcedure<HITSNode>() {
//
//			@Override
//			public boolean execute(HITSNode el) {
//				System.out.println("Node " + el.node_id + ": a=" + el.a_score + ": h=" + el.h_score);
//				return true;
//			}
//			
//		});
	}
	
	public static void main(String[] agrs) {
		load_graph(LINK_GRAPH_FILE);
		HITSCompute();
		printScore();
	}

}
