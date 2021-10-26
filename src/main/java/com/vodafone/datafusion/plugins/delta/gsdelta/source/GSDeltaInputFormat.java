package com.vodafone.datafusion.plugins.delta.gsdelta.source;

import com.vodafone.datafusion.plugins.delta.common.source.DeltaSplit;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import java.util.Collections;
import java.util.List;

public class GSDeltaInputFormat extends InputFormat<NullWritable, GSDeltaRecord> {

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) {
        return Collections.singletonList(new DeltaSplit());
    }

    @Override
    public RecordReader<NullWritable, GSDeltaRecord> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        return new GSDeltaRecordReader();
    }
}
