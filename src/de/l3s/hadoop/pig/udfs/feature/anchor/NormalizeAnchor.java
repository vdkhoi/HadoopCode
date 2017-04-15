package de.l3s.hadoop.pig.udfs.feature.anchor;
import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;

import com.entopix.maui.stopwords.StopwordsEnglish;
import com.entopix.maui.stopwords.StopwordsGerman;

public class NormalizeAnchor extends EvalFunc<String> {
	
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) return null;
        try {
            String str = (String)input.get(0);
            if (str == null || str.length() == 0) return null;
            
            String result = "";
            
            GermanAnalyzer analyzer = new GermanAnalyzer();
			TokenStream tokens = analyzer.tokenStream(null, str);
			CharTermAttribute charTermAttrib = tokens
					.getAttribute(CharTermAttribute.class);
			tokens.reset();
			StopwordsEnglish stopwE = new StopwordsEnglish();
            StopwordsGerman stopwG = new StopwordsGerman();
			while (tokens.incrementToken()) {
				String term = charTermAttrib.toString();
				if (term.length() > 2 && !term.matches("^[-+]?\\d+(\\.\\d+)?$")) {
					if (!stopwE.isStopword(str) && !stopwG.isStopword(str)) {
						result += term + " ";
					}
				}
			}
			tokens.end();
			tokens.close();
			analyzer.close();
            return result;
            
            //String[] meaningless_words = more_stopw.split(" ");
            

        } catch (ClassCastException e) {
            warn(e.getMessage(), PigWarning.UDF_WARNING_1);
            return null;
        }
    }
}