package com.lineindexer;

import com.lineindexer.job1.LineCountJob;
import com.lineindexer.job2.LineIndexJob;
import com.lineindexer.util.OffsetFileBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class LineIndexerDriver {

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("Usage: LineIndexerDriver <inputPath> <tempDir> <finalOutputDir>");
			System.exit(1);
		}

		Path inputPath = new Path(args[0]);
		Path tempDir = new Path(args[1]); // temp working dir
		Path finalOutput = new Path(args[2]); // indexed output

		Path job1Output = new Path(tempDir, "job1_line_counts");
		Path offsetFile = new Path(tempDir, "split_offsets.txt");

		Configuration conf = new Configuration();

		// =========================
		// JOB 1 — Count lines per split
		// =========================
		System.out.println("Starting Job 1: Counting lines per split...");
		Job job1 = LineCountJob.createJob(conf, inputPath, job1Output);

		if (!job1.waitForCompletion(true)) {
			System.err.println("Job 1 failed!");
			System.exit(1);
		}
		System.out.println("Job 1 completed ✔");

		// =========================
		// BUILD OFFSET FILE
		// =========================
		System.out.println("Building offset file...");
		OffsetFileBuilder.buildOffsetFile(conf, job1Output, offsetFile);
		System.out.println("Offset file created at: " + offsetFile);

		// =========================
		// JOB 2 — Assign global index
		// =========================
		System.out.println("Starting Job 2: Global line indexing...");
		boolean sortedOutput = true; // set false if you don't need ordering

		Job job2 = LineIndexJob.createJob(conf, inputPath, finalOutput, offsetFile, sortedOutput);

		if (!job2.waitForCompletion(true)) {
			System.err.println("Job 2 failed!");
			System.exit(1);
		}

		System.out.println("LineIndexer completed successfully 🎉");
	}
}
