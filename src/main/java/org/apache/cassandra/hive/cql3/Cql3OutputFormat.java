package org.apache.cassandra.hive.cql3;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;

public class Cql3OutputFormat extends org.apache.hadoop.mapreduce.OutputFormat<NullWritable, MapWritable> implements org.apache.hadoop.mapred.OutputFormat<NullWritable, MapWritable>,
        HiveOutputFormat<NullWritable, MapWritable> {
    @Override
    public RecordWriter getHiveRecordWriter(org.apache.hadoop.mapred.JobConf conf, Path finalOutPath, Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties, Progressable progress)
            throws IOException {
        throw new UnsupportedOperationException("INSERT not yet supported to Cassandra");
    }

    // mapred
    @Override
    public void checkOutputSpecs(FileSystem arg0, org.apache.hadoop.mapred.JobConf conf) throws IOException {
        throw new UnsupportedOperationException("INSERT not yet supported to Cassandra");
    }

    @Override
    public org.apache.hadoop.mapred.RecordWriter<NullWritable, MapWritable> getRecordWriter(FileSystem arg0, org.apache.hadoop.mapred.JobConf arg1, String arg2, Progressable arg3) throws IOException {
        throw new RuntimeException("Error: Hive should not invoke this method.");
    }

    // mapreduce
    @Override
    public void checkOutputSpecs(org.apache.hadoop.mapreduce.JobContext arg0) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("INSERT not yet supported to Cassandra");
    }

    @Override
    public org.apache.hadoop.mapreduce.OutputCommitter getOutputCommitter(org.apache.hadoop.mapreduce.TaskAttemptContext arg0) throws IOException, InterruptedException {
        throw new RuntimeException("Error: Hive should not invoke this method.");
    }

    @Override
    public org.apache.hadoop.mapreduce.RecordWriter<NullWritable, MapWritable> getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext arg0) throws IOException, InterruptedException {
        throw new RuntimeException("Error: Hive should not invoke this method.");
    }
}
