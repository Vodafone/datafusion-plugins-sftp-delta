package com.vodafone.datafusion.plugins.sftpdelta.source;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import java.util.Collections;
import java.util.List;


public class SFTPDeltaInputFormat extends InputFormat<NullWritable, SFTPDeltaRecord> {

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) {
        return Collections.singletonList(new SFTPDeltaSplit());
    }

    @Override
    public RecordReader<NullWritable, SFTPDeltaRecord> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        return new SFTPDeltaRecordReader();
    }
}
