/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper.grok;


import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.prepper.AbstractPrepper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@DataPrepperPlugin(name = "grok", type = PluginType.PREPPER)
public class GrokPrepper extends AbstractPrepper<Record<String>, Record<String>> {

    private static final Logger LOG = LoggerFactory.getLogger(GrokPrepper.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};

    private final GrokCompiler grokCompiler;
    private final Map<String, List<Grok>> fieldToGrok;
    private final GrokPrepperConfig grokPrepperConfig;

    public GrokPrepper(final PluginSetting pluginSetting) {
        super(pluginSetting);
        grokPrepperConfig = GrokPrepperConfig.buildConfig(pluginSetting);
        grokCompiler = GrokCompiler.newInstance();
        fieldToGrok = new LinkedHashMap<>();

        registerPatterns();

        for (Map.Entry<String, List<String>> entry : grokPrepperConfig.getMatch().entrySet()) {
            fieldToGrok.put(entry.getKey(), entry.getValue().stream().map(item -> grokCompiler.compile(item, grokPrepperConfig.isNamedCapturesOnly())).collect(Collectors.toList()));
        }
    }

    /**
     * execute the prepper logic which could potentially modify the incoming record. The level to which the record has
     * been modified depends on the implementation
     *
     * @param records Input records that will be modified/processed
     * @return Record  modified output records
     */
    @Override
    public Collection<Record<String>> doExecute(Collection<Record<String>> records) {
        final List<Record<String>> recordsOut = new LinkedList<>();

        for (Record<String> record : records) {
            try {
                final Map<String, Object> recordMap = OBJECT_MAPPER.readValue(record.getData(), MAP_TYPE_REFERENCE);
                final Map<String, Object> grokkedLog = new HashMap<>();

                for (Map.Entry<String, List<Grok>> entry : fieldToGrok.entrySet()) {
                    for (Grok grok : entry.getValue()) {
                        if (recordMap.containsKey(entry.getKey())) {
                            Match match = grok.match(recordMap.get(entry.getKey()).toString());
                            match.setKeepEmptyCaptures(grokPrepperConfig.isKeepEmptyCaptures());

                            mergeUpdateWithOriginalMap(grokkedLog, match.capture());
                        }
                    }
                }

                mergeUpdateWithOriginalMap(recordMap, grokkedLog);

                final Record<String> grokkedRecord = new Record<>(OBJECT_MAPPER.writeValueAsString(recordMap), record.getMetadata());
                recordsOut.add(grokkedRecord);

            } catch (JsonProcessingException e) {
                LOG.error("Failed to parse the record [{}]", record.getData());
                recordsOut.add(record);
            }
        }
        return recordsOut;
    }

    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {

    }

    private void registerPatterns() {
        grokCompiler.registerDefaultPatterns();
    }

    /**
     * merge key value pairs from updates Map with the original Map. If keys in the updates Map already exist in the original Map,
     * the value for that key in the original Map will be made a List<String>, and the values from updates will be appended. For collisions
     * where the value is not of type String, updates will be ignored
     *
     * @param original The original Map to be updated with values from key value pairs from updates
     * @param updates The updates Map that contains key value pairs to be merged with original
     */
    private void mergeUpdateWithOriginalMap(final Map<String, Object> original, final Map<String, Object> updates) {
        for (Map.Entry<String, Object> updateEntry : updates.entrySet()) {

            if (!(original.containsKey(updateEntry.getKey()))) {
                original.put(updateEntry.getKey(), updateEntry.getValue());
                continue;
            }

            if (List.class.isAssignableFrom(original.get(updateEntry.getKey()).getClass())) {
                ((List<Object>) original.get(updateEntry.getKey())).add(updateEntry.getValue());
            } else {
                List<Object> values = new ArrayList<>(Collections.singletonList(original.get(updateEntry.getKey())));
                values.add(updateEntry.getValue());
                original.put(updateEntry.getKey(), values);
            }
        }
    }
}