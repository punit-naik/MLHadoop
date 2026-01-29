package com.lineindexer.job2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer
        extends Reducer<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {

        for (Text value : values) {
            ctx.write(key, value);
        }
    }
}