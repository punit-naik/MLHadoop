package com.lineindexer.job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LineIndexJob {

    public static Job createJob(Configuration conf,
                                Path inputPath,
                                Path outputPath,
                                Path offsetFilePath,
                                boolean sortedOutput) throws Exception {

        Job job = Job.getInstance(conf, "LineIndexer - Job2 - Global Line Indexing");

        job.setJarByClass(LineIndexJob.class);
        job.setMapperClass(LineIndexingMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.addCacheFile(offsetFilePath.toUri());

        if (sortedOutput) {
            job.setReducerClass(IndexReducer.class);
            job.setNumReduceTasks(1);
        } else {
            job.setNumReduceTasks(0);
        }

        return job;
    }
}