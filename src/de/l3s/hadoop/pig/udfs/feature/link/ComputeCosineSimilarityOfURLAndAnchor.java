package de.l3s.hadoop.pig.udfs.feature.link;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


class DocumentVector {
    TObjectIntHashMap<String> wordMap = new TObjectIntHashMap<String>();

    public void incCount(String word) {
        Integer oldCount = wordMap.get(word);
        wordMap.put(word, oldCount == null ? 1 : oldCount + 1);
    }

    double getCosineSimilarityWith(DocumentVector otherVector) {
        double innerProduct = 0;
        for(String w: this.wordMap.keySet()) {
            innerProduct += this.getCount(w) * otherVector.getCount(w);
        }
        return innerProduct / (this.getNorm() * otherVector.getNorm());
    }

    double getNorm() {
        double sum = 0;
        for (Integer count : wordMap.values()) {
            sum += count * count;
        }
        return Math.sqrt(sum);
    }

    int getCount(String word) {
        return wordMap.containsKey(word) ? wordMap.get(word) : 0;
    }

    public DocumentVector(String doc) {
        for(String w:doc.split("[^a-zA-Z]+")) {
            incCount(w);
        }
    }

}



public class ComputeCosineSimilarityOfURLAndAnchor extends EvalFunc<Double> {
	public Double exec(Tuple input) throws IOException {
		
		String url = "", anchors = "";
		
		if (input == null) 
			return new Double(0.0f);
		if (input.size() != 2)
			return new Double(0.0f);
		else {
			
			if (input.get(0) == null) return new Double(0.0f);
			if (input.get(1) == null) return new Double(0.0f);
			
			url = input.get(0).toString();
			anchors = input.get(1).toString();
			
			if (url.length() == 0) return new Double(0.0f);
			if (anchors.length() == 0) return new Double(0.0f);
			
		}
		
		try {
			DocumentVector v1 = new DocumentVector(input.get(0).toString());
			DocumentVector v2 = new DocumentVector(input.get(1).toString());
			return v1.getCosineSimilarityWith(v2);
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + input.toString(), e);
		}
	}
}
