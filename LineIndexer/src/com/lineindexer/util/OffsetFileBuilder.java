package com.lineindexer.util;

import java.io.*;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class OffsetFileBuilder {

    public static Path buildOffsetFile(Configuration conf,
                                       Path job1OutputDir,
                                       Path offsetOutputPath) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        TreeMap<Long, Long> splitLineCounts = new TreeMap<>();

        FileStatus[] files = fs.listStatus(job1OutputDir);
        for (FileStatus status : files) {

            if (!status.getPath().getName().startsWith("part-")) continue;

            FSDataInputStream in = fs.open(status.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                String splitId = parts[0];
                long count = Long.parseLong(parts[1]);

                long splitStart = Long.parseLong(
                        splitId.substring(splitId.lastIndexOf("_") + 1)
                );

                splitLineCounts.put(splitStart, count);
            }
            br.close();
        }

        FSDataOutputStream out = fs.create(offsetOutputPath, true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

        long runningTotal = 0;

        for (Map.Entry<Long, Long> entry : splitLineCounts.entrySet()) {
            long splitStart = entry.getKey();
            long count = entry.getValue();
            String splitId = "data.txt_" + splitStart;

            bw.write(splitId + "\t" + runningTotal + "\n");
            runningTotal += count;
        }

        bw.close();
        return offsetOutputPath;
    }
}