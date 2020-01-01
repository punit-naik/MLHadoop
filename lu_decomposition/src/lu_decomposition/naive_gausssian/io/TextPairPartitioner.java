package lu_decomposition.naive_gausssian.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextPairPartitioner extends Partitioner<TextPair, Text>{
    @Override
    public int getPartition(TextPair tp, Text t, int numPartitions) {
        return tp.getFirst().hashCode() % numPartitions;
    }
}