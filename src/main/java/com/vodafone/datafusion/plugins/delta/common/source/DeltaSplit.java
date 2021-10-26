package com.vodafone.datafusion.plugins.delta.common.source;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * No Split necessary
 */
public class DeltaSplit extends InputSplit implements Writable {
    @Override
    public void write(DataOutput dataOutput) {

    }

    @Override
    public void readFields(DataInput dataInput) {

    }

    @Override
    public long getLength() {
        return 0;
    }

    @Override
    public String[] getLocations() {
        return new String[0];
    }
}
