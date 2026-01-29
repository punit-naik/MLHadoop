# LineIndexer

LineIndexer is a Hadoop MapReduce project that implements functionality similar to Spark’s **`zipWithIndex()`** for very large text files stored in HDFS.  
It assigns a **global line number (starting from 0)** to every line in a distributed file.

---

## Objective

In Hadoop, input files are split into multiple blocks and processed by different mappers.  
Each mapper only sees its own split, so it does not know how many lines came before it.

This project solves that problem and produces output of the form:

```
0    first line
1    second line
2    third line
...
```

for extremely large files.

---

## Approach

The solution uses **two MapReduce jobs** and a prefix-sum computation.

### Job 1 – Count Lines per Split
Each mapper:
- Processes one input split
- Counts the number of lines in that split
- Outputs:

```
<splitId, lineCount>
```

Example:

```
data.txt_0            13456789
data.txt_134217728    12999876
```

No reducer is used in Job 1.

---

### Offset Builder (Driver Step)

After Job 1 completes, the driver:

1. Reads all split line counts  
2. Sorts splits by their starting byte offset  
3. Computes prefix sums  

This produces an offset file:

```
data.txt_0            0
data.txt_134217728    13456789
data.txt_268435456    26456665
```

This file is distributed to all mappers in Job 2.

---

### Job 2 – Global Line Indexing

Each mapper:

- Loads its split offset
- Maintains a local counter
- Computes:

```
globalIndex = splitOffset + localIndex
```

Output:

```
0    first line
1    second line
2    third line
...
```

A reducer is optional:

- **0 reducers** → faster, output not globally ordered  
- **1 reducer** → globally sorted output  

---

## Project Structure

```
com.lineindexer
│
├── job1
│   ├── SplitLineCountMapper.java
│   └── LineCountJob.java
│
├── job2
│   ├── LineIndexingMapper.java
│   ├── LineIndexJob.java
│   └── IndexReducer.java
│
└── util
    └── OffsetFileBuilder.java

LineIndexerDriver.java
```

---

## How to Run

### Build

```
mvn clean package
```

### Execute

```
hadoop jar LineIndexer.jar   com.lineindexer.LineIndexerDriver   /input/hugefile.txt   /tmp/lineindexer-work   /output/indexed-file
```

---

## Arguments

| Argument | Description |
|----------|-------------|
| inputPath | HDFS path of original file |
| tempDir | Temporary working directory |
| finalOutputDir | Final indexed output path |

---

## Key Concepts Used

- Map-only job  
- Prefix sum (scan)  
- Distributed cache  
- Split-aware processing  
- Optional global sort using reducer  

---

## Notes

- Works best with splittable formats (e.g., plain text).
- Gzip files are not splittable → only one mapper will run.
- For extremely large outputs with sorting, a single reducer can be a bottleneck.

---

## Summary

LineIndexer demonstrates how to implement **global indexing of records in Hadoop** using multiple MapReduce jobs and a distributed prefix-sum strategy.
