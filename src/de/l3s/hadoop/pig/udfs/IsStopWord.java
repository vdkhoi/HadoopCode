package de.l3s.hadoop.pig.udfs;
import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;
import com.entopix.maui.stopwords.StopwordsEnglish;
import com.entopix.maui.stopwords.StopwordsGerman;

public class IsStopWord extends FilterFunc {

	
	
    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) return false;
        try {
            String str = (String)input.get(0);
            if (str == null || str.length() == 0) return false;
            
            StopwordsEnglish stopwE = new StopwordsEnglish();
            StopwordsGerman stopwG = new StopwordsGerman();

            return stopwE.isStopword(str) || stopwG.isStopword(str);
            
            //String[] meaningless_words = more_stopw.split(" ");
            

        } catch (ClassCastException e) {
            warn(e.getMessage(), PigWarning.UDF_WARNING_1);
            return false;
        }
    }
}