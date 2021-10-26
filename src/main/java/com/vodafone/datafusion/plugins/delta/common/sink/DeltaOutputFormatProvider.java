package com.vodafone.datafusion.plugins.delta.common.sink;

import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * Output format provider for SFTPDeltaGCS Sink.
 */
public class DeltaOutputFormatProvider implements OutputFormatProvider {
    private Map<String, String> conf;

    public DeltaOutputFormatProvider(BatchSinkContext context) {
        conf = new HashMap<>();
    }

    @Override
    public String getOutputFormatClassName() {
        return NullOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
        return conf;
    }
}
