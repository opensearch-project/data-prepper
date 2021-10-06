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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
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
        compileMatchPatterns();
    }

    /**
     * execute the prepper logic which could potentially modify the incoming record. The level to which the record has
     * been modified depends on the implementation
     *
     * @param records Input records that will be modified/processed
     * @return Record  modified output records
     */
    @Override
    public Collection<Record<String>> doExecute(final Collection<Record<String>> records) {
        final List<Record<String>> recordsOut = new LinkedList<>();

        for (final Record<String> record : records) {
            boolean foundMatch = false;

            try {
                final Map<String, Object> recordMap = OBJECT_MAPPER.readValue(record.getData(), MAP_TYPE_REFERENCE);
                final Map<String, Object> grokkedLog = new HashMap<>();

                for (final Map.Entry<String, List<Grok>> entry : fieldToGrok.entrySet()) {
                    for (final Grok grok : entry.getValue()) {
                        if (recordMap.containsKey(entry.getKey())) {
                            final Match match = grok.match(recordMap.get(entry.getKey()).toString());
                            match.setKeepEmptyCaptures(grokPrepperConfig.isKeepEmptyCaptures());

                            final Map<String, Object> captures = match.capture();
                            mergeCaptures(grokkedLog, captures);

                            if (captures.size() > 0 && grokPrepperConfig.isBreakOnMatch()) {
                                foundMatch = true;
                                break;
                            }
                        }
                    }
                    if (foundMatch) {
                        break;
                    }
                }

                if (grokPrepperConfig.getTarget() != null) {
                    recordMap.put(grokPrepperConfig.getTarget(), grokkedLog);
                } else {
                    mergeCaptures(recordMap, grokkedLog);
                }

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
        grokCompiler.register(grokPrepperConfig.getPatternDefinitions());
        registerPatternsDir();
    }

    private void registerPatternsDir() {
        final Pattern compiledFilesGlob = Pattern.compile(grokPrepperConfig.getPatternsFilesGlob());
        for (final String directory : grokPrepperConfig.getPatternsDir()) {
            try {
                final File dir = new File(directory);
                final File[] dirFiles = dir.listFiles();

                if (dirFiles != null) {
                    for (final File file : dirFiles) {
                        if (compiledFilesGlob.matcher(file.getName()).find()) {
                            registerPatternsForFile(file);
                        }
                    }
                } else {
                    LOG.info("Directory {} is empty", directory);
                }
            } catch (NullPointerException e) {
                LOG.error("Pattern directory is null", e);
            }
        }
    }

    private void registerPatternsForFile(final File file) {
        try (final InputStream in = new FileInputStream(file)) {
            grokCompiler.register(in);
        } catch (FileNotFoundException e) {
            LOG.error("Pattern file {} not found", file, e);
        } catch (IOException e) {
            LOG.error("Error reading from pattern file {}", file, e);
        }
    }

    private void compileMatchPatterns() {
        for (final Map.Entry<String, List<String>> entry : grokPrepperConfig.getMatch().entrySet()) {
            fieldToGrok.put(entry.getKey(), entry.getValue()
                            .stream()
                            .map(item -> grokCompiler.compile(item, grokPrepperConfig.isNamedCapturesOnly()))
                            .collect(Collectors.toList()));
        }
    }

    private void mergeCaptures(final Map<String, Object> original, final Map<String, Object> updates) {
        for (final Map.Entry<String, Object> updateEntry : updates.entrySet()) {
            if (!(original.containsKey(updateEntry.getKey())) || grokPrepperConfig.getOverwrite().contains(updateEntry.getKey())) {
                original.put(updateEntry.getKey(), updateEntry.getValue());
                continue;
            }

            if (original.get(updateEntry.getKey()) instanceof List) {
                mergeValueWithValues(updateEntry.getValue(), (List<Object>) original.get(updateEntry.getKey()));
            } else {
                final List<Object> values = new ArrayList<>(Collections.singletonList(original.get(updateEntry.getKey())));
                mergeValueWithValues(updateEntry.getValue(), values);
                original.put(updateEntry.getKey(), values);
            }
        }
    }

    private void mergeValueWithValues(final Object value, final List<Object> values) {
        if (value instanceof List) {
            values.addAll((List<Object>) value);
        } else {
            values.add(value);
        }
    }
}