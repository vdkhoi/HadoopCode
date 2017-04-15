package de.l3s.hadoop.mapreduce.graph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Random;

class RandomString {

	private static final char[] symbols;

	static {
		StringBuilder tmp = new StringBuilder();
		for (char ch = '0'; ch <= '9'; ++ch)
			tmp.append(ch);
		for (char ch = 'a'; ch <= 'z'; ++ch)
			tmp.append(ch);
		symbols = tmp.toString().toCharArray();
	}

	private final Random random = new Random();

	private final char[] buf;

	public RandomString(int length) {
		if (length < 1)
			throw new IllegalArgumentException("length < 1: " + length);
		buf = new char[length];
	}

	public String nextString() {
		for (int idx = 0; idx < buf.length; ++idx)
			buf[idx] = symbols[random.nextInt(symbols.length)];
		return new String(buf);
	}
}

public class LocalGenerateSampleGraph {
	public static final int GRAPH_SIZE = 1000000;
	public static final String NODE_LIST_FILE = "Z:/Alexandria/Results/Result_follow_up_WebSci16/random_graph_for_shs/node_list";
	public static final String LINK_FILE = "Z:/Alexandria/Results/Result_follow_up_WebSci16/random_graph_for_shs/links";

	public static void main(String[] agrs) {
		File out = new File(NODE_LIST_FILE);
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(out));
			RandomString rand = new RandomString(20);
			double j = 0;
			String url = "";
			for (int i = 0; i < GRAPH_SIZE; i ++) {
				j = Math.random();
				url = ((j < 0.5)? "http://www." : "https://www.") + rand.nextString() + ".de/";
				bw.write(url + "\r\n");
			}
			bw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			
			out = new File(LINK_FILE);
			bw = new BufferedWriter(new FileWriter(out));
			
			Random rn1 = new Random(100), rn2 = new Random(GRAPH_SIZE);
			String list = "";
			int adj_size = 0, k = 0;
			for (int i = 0; i < GRAPH_SIZE; i ++) {
				list = "";
				adj_size = Math.abs(rn1.nextInt() % 100); 
				for (int j = 0; j < adj_size; j ++) {
					k = Math.abs(rn2.nextInt() % GRAPH_SIZE);
					list += (k + " ");
				}
				k = Math.abs(rn2.nextInt() % GRAPH_SIZE);
				list += (k);
				bw.write(list + "\r\n");

			}
			bw.close();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}

		
	}
}
