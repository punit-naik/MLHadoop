package com.lineindexer.job1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SplitLineCountMapper
        extends Mapper<LongWritable, Text, Text, LongWritable> {

    private long lineCount = 0;
    private String splitId;

    @Override
    protected void setup(Context context) {
        FileSplit split = (FileSplit) context.getInputSplit();
        String fileName = split.getPath().getName();
        long splitStart = split.getStart();
        splitId = fileName + "_" + splitStart;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        lineCount++;
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        context.write(new Text(splitId), new LongWritable(lineCount));
    }
}