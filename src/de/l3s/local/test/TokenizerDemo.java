package de.l3s.local.test;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.util.List;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.process.PTBTokenizer;

public class TokenizerDemo {

	public static void main(String[] args) throws IOException {
		String[] test = {"Kurz nachdem Mumay verhaftet wurde, sind auch 47 weitere Haftbefehle gegen frühere Mitarbeiter der Zeitung „Zaman“ ergangen, eine der Gülen-Bewegung verbundenen Publikation, die schon im März unter Zwangsverwaltung gestellt worden war", 
						 "https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-lang-analyzer.html"};
		for (String arg : test) {
			// option #1: By sentence.
			DocumentPreprocessor dp = new DocumentPreprocessor(new StringReader(arg));
			for (List<HasWord> sentence : dp) {
				System.out.println(sentence);
			}
			// option #2: By token
			PTBTokenizer<CoreLabel> ptbt = new PTBTokenizer<CoreLabel>(new StringReader(
					arg), new CoreLabelTokenFactory(), "");
			while (ptbt.hasNext()) {
				CoreLabel label = ptbt.next();
				System.out.println(label);
			}
		}
		
		
		URL url = new URL(test[1]); 
		System.out.println("Authority: " + url.getAuthority());
		System.out.println("File: " + url.getFile());
		System.out.println("Path: " + url.getPath());
		System.out.println("Query: " + url.getQuery());
		System.out.println("Ref: " + url.getRef());
	}
}