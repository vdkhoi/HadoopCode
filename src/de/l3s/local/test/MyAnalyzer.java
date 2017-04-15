package de.l3s.local.test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class MyAnalyzer {
	public static void main(String[] agrs) {

		try {
			String test = "wyss samen und pflanzen wyss samen und pflanzen wyss samen und pflanzen wyss samen und pflanzen wyss samen und pflanzen wyss samen und pflanzen wyss samen und pflanzen wyss samen und pflanzen wyss samen und pflanzen wyss samen und pflanzen wyss samen und pflanzen wyss samen und pflanzen wyss samen und pflanzen wyss samen und pflanzen ";

			Tokenizer tokenizer = new StandardTokenizer(new StringReader(test));
			CharTermAttribute charTermAttrib = tokenizer
					.getAttribute(CharTermAttribute.class);
			TypeAttribute typeAtt = tokenizer.getAttribute(TypeAttribute.class);
			OffsetAttribute offset = tokenizer
					.getAttribute(OffsetAttribute.class);
			
			KeywordAttribute keyAttr = tokenizer.addAttribute(KeywordAttribute.class);
			
			//FlagsAttribute flag = tokenizer.getAttribute(FlagsAttribute.class); 
			
			
			List<String> tokens = new ArrayList<String>();
			tokenizer.reset();
			int term_num = 0;
			while (tokenizer.incrementToken()) {
				tokens.add(charTermAttrib.toString());
				System.out.println("Term " + (term_num++) + ": " + charTermAttrib.toString());
			}
			tokenizer.end();
			tokenizer.close();

			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
