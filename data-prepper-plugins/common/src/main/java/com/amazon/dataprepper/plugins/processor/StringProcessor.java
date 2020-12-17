package com.amazon.dataprepper.plugins.processor;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collection;

/**
 * An simple String implementation of {@link Processor} which generates new Records with upper case or lowercase content. The current
 * simpler implementation does not handle errors (if any).
 */
@DataPrepperPlugin(name = "string_converter", type = PluginType.PROCESSOR)
public class StringProcessor implements Processor<Record<String>, Record<String>> {

    private final boolean upperCase;

    /**
     * Mandatory constructor for Data Prepper Component - This constructor is used by Data Prepper
     * runtime engine to construct an instance of {@link StringProcessor} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public StringProcessor(final PluginSetting pluginSetting) {
        this.upperCase = pluginSetting.getBooleanOrDefault("upper_case", true);
    }

    @Override
    public Collection<Record<String>> execute(final Collection<Record<String>> records) {
        final Collection<Record<String>> modifiedRecords = new ArrayList<>(records.size());
        for (Record<String> record : records) {
            final String recordData = record.getData();
            final String newData = upperCase? recordData.toUpperCase() : recordData.toLowerCase();
            modifiedRecords.add(new Record<>(newData));
        }
        return modifiedRecords;
    }

    @Override
    public void shutdown() {

    }
}
