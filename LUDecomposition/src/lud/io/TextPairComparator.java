package lud.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextPairComparator extends WritableComparator {
    protected TextPairComparator() {
        super(TextPair.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    	TextPair tp1 = (TextPair)w1;
    	TextPair tp2 = (TextPair)w2;
         
        int result = tp1.getFirst().compareTo(tp2.getFirst());
        if(0 == result) {
            result = tp1.getSecond().compareTo(tp2.getSecond());
        }
        return result;
    }
}