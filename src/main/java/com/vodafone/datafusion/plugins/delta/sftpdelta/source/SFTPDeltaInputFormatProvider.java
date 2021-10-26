package com.vodafone.datafusion.plugins.delta.sftpdelta.source;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.plugin.PluginConfig;

import java.util.Map;
import static com.vodafone.datafusion.plugins.delta.constants.Constants.CONF_JSON_PACKAGE_KEY;


public class SFTPDeltaInputFormatProvider implements InputFormatProvider {
    private static final Gson gson = new GsonBuilder().create();

    private final PluginConfig config;
    private final Map<String, String> conf;

    public SFTPDeltaInputFormatProvider(PluginConfig config) {
        this.config = config;

        this.conf = new ImmutableMap.Builder<String, String>()
                .put(CONF_JSON_PACKAGE_KEY, gson.toJson(config))
                .build();
    }

    @Override
    public String getInputFormatClassName() {
        return SFTPDeltaInputFormat.class.getName();
    }

    @Override
    public Map<String, String> getInputFormatConfiguration() {
        return conf;
    }
}
