package com.lineindexer.job2;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LineIndexingMapper
        extends Mapper<LongWritable, Text, LongWritable, Text> {

    private long splitOffset = 0;
    private long localIndex = 0;

    @Override
    protected void setup(Context context) throws IOException {
        FileSplit split = (FileSplit) context.getInputSplit();
        String splitId = split.getPath().getName() + "_" + split.getStart();

        Map<String, Long> offsetMap = new HashMap<>();
        URI[] cacheFiles = context.getCacheFiles();

        for (URI uri : cacheFiles) {
            BufferedReader br = new BufferedReader(new FileReader(new File(uri.getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                offsetMap.put(parts[0], Long.parseLong(parts[1]));
            }
            br.close();
        }

        splitOffset = offsetMap.get(splitId);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        long globalIndex = splitOffset + localIndex;
        context.write(new LongWritable(globalIndex), value);
        localIndex++;
    }
}