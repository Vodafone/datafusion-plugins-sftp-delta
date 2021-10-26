package com.vodafone.datafusion.plugins.delta.gsdelta.source;

import com.google.common.base.Strings;
import com.vodafone.datafusion.plugins.delta.common.DeltaUtils;
import com.vodafone.datafusion.plugins.delta.common.source.DeltaDelta;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * An {@link BatchSource} that will create a list of files from GCS.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(GSDeltaSource.NAME)
@Description("GSDeltaSource: list files from GCS")
public class GSDeltaSource extends BatchSource<NullWritable, GSDeltaRecord, StructuredRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(GSDeltaSource.class);

    public static final String NAME = "GSDeltaSource";
    private final GSDeltaSourceConfig config;

    public static final Schema outputSchema = Schema.recordOf("textRecord",
            Schema.Field.of("connectionType", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("connection", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("fullfilename", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("filename", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("basename", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("size", Schema.of(Schema.Type.LONG)),
            Schema.Field.of("mtime", Schema.of(Schema.Type.INT))
    );

    public GSDeltaSource(GSDeltaSourceConfig config) {
        this.config = config;
    }

    @Override
    public void initialize(BatchRuntimeContext context) throws Exception {
        LOG.debug("[SFTP Delta] Initializing source.");
        super.initialize(context);
    }

    @Override
    public void configurePipeline(PipelineConfigurer configurer) {
        super.configurePipeline(configurer);
        StageConfigurer stageConfigurer = configurer.getStageConfigurer();
        FailureCollector collector = stageConfigurer.getFailureCollector();
        config.validate(collector);

        configurer.getStageConfigurer().setOutputSchema(outputSchema);
    }

    @Override
    public void prepareRun(BatchSourceContext context) throws Exception {
        LOG.debug("[SFTP Delta] Source prepareRun.");
        try {
            MDC.put("pluginUuid", context.getMetrics().getTags().get("wfr"));
            FailureCollector collector = context.getFailureCollector();
            config.validate(collector);
            collector.getOrThrowException();
            context.setInput(Input.of(config.referenceName, new GSDeltaInputFormatProvider(config)));
        } catch (Exception ex) {
            LOG.error("[SFTP Delta] Error on source. prepareRun." + ex.getMessage());
            throw new Exception(ex.getMessage());
        }
    }

    @Override
    public void onRunFinish(boolean succeeded, BatchSourceContext context) {
        if (succeeded && !context.isPreviewEnabled() && !Strings.isNullOrEmpty(config.persistDelta)) {
            try {
                Long safetyTime = DeltaUtils.getSafetyTime(context);
                Long toTime = (context.getLogicalStartTime() / 1000) - safetyTime;
                DeltaDelta.setPersistTime(toTime, config);
                LOG.info("[SFTP Delta] Updated delta file.");
            } catch (Exception ex) {
                LOG.error("[SFTP Delta] Error persisting Delta: {} - {}", config.persistDelta, ex.getMessage());
                ex.printStackTrace();
            }
        }
    }

    @Override
    public void transform(KeyValue<NullWritable, GSDeltaRecord> input, Emitter<StructuredRecord> emitter) {
        GSDeltaRecord info = input.getValue();
        emitter.emit(StructuredRecord.builder(outputSchema)
                .set("connectionType", info.connectionType)
                .set("connection", info.connection)
                .set("fullfilename", info.fullfilename)
                .set("filename", info.filename)
                .set("basename", info.basename)
                .set("size", info.size)
                .set("mtime", info.mTime)
                .build()
        );
    }
}
