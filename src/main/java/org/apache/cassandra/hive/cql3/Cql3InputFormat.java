package org.apache.cassandra.hive.cql3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cql3InputFormat extends org.apache.hadoop.mapreduce.InputFormat<NullWritable, MapWritable> implements org.apache.hadoop.mapred.InputFormat<NullWritable, MapWritable> {
    static Logger LOG = LoggerFactory.getLogger(Cql3StorageHandler.class);
    public static final String MAPRED_TASK_ID = "mapred.task.id";

    // package: mapred

    public org.apache.hadoop.mapred.RecordReader<NullWritable, MapWritable> getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf jobConf, final Reporter reporter) throws IOException {
        TaskAttemptContext tac = new TaskAttemptContext(jobConf, new TaskAttemptID()) { // TaskAttemptID.forName(jobConf.get(MAPRED_TASK_ID))
            @Override
            public void progress() {
                reporter.progress();
            }
        };

        Cql3RecordReader recordReader = new Cql3RecordReader();
        recordReader.initialize((org.apache.hadoop.mapreduce.InputSplit) split, tac);
        return recordReader;
    }

    public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        TaskAttemptContext tac = new TaskAttemptContext(jobConf, new TaskAttemptID());
        List<org.apache.hadoop.mapreduce.InputSplit> newInputSplits = this.getSplits(tac);
        org.apache.hadoop.mapred.InputSplit[] oldInputSplits = new org.apache.hadoop.mapred.InputSplit[newInputSplits.size()];
        for (int i = 0; i < newInputSplits.size(); i++)
            oldInputSplits[i] = (Cql3Split) newInputSplits.get(i);
        return oldInputSplits;
    }

    // package: mapreduce

    @Override
    public org.apache.hadoop.mapreduce.RecordReader<NullWritable, MapWritable> createRecordReader(org.apache.hadoop.mapreduce.InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
        return new Cql3RecordReader();
    }

    @Override
    public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(JobContext context) throws IOException {
        // Configuration conf = context.getConfiguration();
        List<org.apache.hadoop.mapreduce.InputSplit> splits = new ArrayList<org.apache.hadoop.mapreduce.InputSplit>();
        splits.add(new Cql3Split());
        return splits;
    }
}
