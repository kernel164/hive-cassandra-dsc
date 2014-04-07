package org.apache.cassandra.hive.cql3;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cql3RecordReader extends org.apache.hadoop.mapreduce.RecordReader<NullWritable, MapWritable> implements org.apache.hadoop.mapred.RecordReader<NullWritable, MapWritable> {
    static Logger LOG = LoggerFactory.getLogger(Cql3StorageHandler.class);
    private Cql3Split split;
    private Iterator<Map<String, Object>> it;
    private long pos;
    private NullWritable key = NullWritable.get();
    private MapWritable value = new MapWritable();
    private Configuration conf;

    @Override
    public void initialize(org.apache.hadoop.mapreduce.InputSplit split, org.apache.hadoop.mapreduce.TaskAttemptContext context) throws IOException {
        this.split = (Cql3Split) split;
        this.conf = context.getConfiguration();
        this.it = HiveCassandraUtils.getRecordsAsMapIterator(conf);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        return it.hasNext();
    }

    @Override
    public boolean next(NullWritable key, MapWritable valuesMap) throws IOException {
        if (nextKeyValue()) {
            Map<String, Object> vMap = it.next();
            for (Entry<String, Object> entry : vMap.entrySet()) {
                valuesMap.put(new Text(entry.getKey()), (entry.getValue() == null) ? NullWritable.get() : new Text(entry.getValue().toString()));
            }
            pos++;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException {
        return key;
    }

    @Override
    public NullWritable createKey() {
        return key;
    }

    @Override
    public MapWritable createValue() {
        value.clear();
        return value;
    }

    @Override
    public MapWritable getCurrentValue() {
        return value;
    }

    @Override
    public long getPos() throws IOException {
        return this.pos;
    }

    @Override
    public float getProgress() throws IOException {
        return split.getLength() > 0 ? pos / (float) split.getLength() : 1.0f;
    }

    @Override
    public void close() throws IOException {
        HiveCassandraUtils.closeCassandraCluster(this.conf);
    }

}
