package de.l3s.hadoop.pig.udfs;
import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;


public class IsNumeric extends FilterFunc {

    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) return false;
        try {
            String str = (String)input.get(0);
            if (str == null || str.length() == 0) return false;

            if (str.startsWith("-")) str = str.substring(1);

            return str.matches("\\d+(\\.\\d+)?");

        } catch (ClassCastException e) {
            warn(e.getMessage(), PigWarning.UDF_WARNING_1);
            return false;
        }
    }
}