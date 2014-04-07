package org.apache.cassandra.hive.cql3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Cql3Split extends org.apache.hadoop.mapreduce.InputSplit implements org.apache.hadoop.mapred.InputSplit {

    @Override
    public long getLength() throws IOException {
        return 1;
    }

    @Override
    public String[] getLocations() throws IOException {
        return new String[0];
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

}
