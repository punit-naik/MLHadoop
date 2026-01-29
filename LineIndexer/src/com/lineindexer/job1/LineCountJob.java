package com.lineindexer.job1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LineCountJob {

    public static Job createJob(Configuration conf, Path inputPath, Path outputPath)
            throws Exception {

        Job job = Job.getInstance(conf, "LineIndexer - Job1 - Split Line Count");
        job.setJarByClass(LineCountJob.class);
        job.setMapperClass(SplitLineCountMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }
}