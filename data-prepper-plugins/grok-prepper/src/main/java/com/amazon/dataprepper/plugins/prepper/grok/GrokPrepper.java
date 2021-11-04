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


import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.prepper.AbstractPrepper;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.record.Record;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
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
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;


@DataPrepperPlugin(name = "grok", pluginType = Prepper.class)
public class GrokPrepper extends AbstractPrepper<Record<Event>, Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(GrokPrepper.class);

    static final String GROK_PROCESSING_MATCH_SUCCESS = "grokProcessingMatchSuccess";
    static final String GROK_PROCESSING_MATCH_FAILURE = "grokProcessingMatchFailure";
    static final String GROK_PROCESSING_ERRORS = "grokProcessingErrors";
    static final String GROK_PROCESSING_TIMEOUTS = "grokProcessingTimeouts";
    static final String GROK_PROCESSING_TIME = "grokProcessingTime";

    private final Counter grokProcessingMatchFailureCounter;
    private final Counter grokProcessingMatchSuccessCounter;
    private final Counter grokProcessingErrorsCounter;
    private final Counter grokProcessingTimeoutsCounter;
    private final Timer grokProcessingTime;

    private final GrokCompiler grokCompiler;
    private final Map<String, List<Grok>> fieldToGrok;
    private final GrokPrepperConfig grokPrepperConfig;
    private final Set<String> keysToOverwrite;
    private final ExecutorService executorService;

    public GrokPrepper(final PluginSetting pluginSetting) {
        this(pluginSetting, GrokCompiler.newInstance(), Executors.newSingleThreadExecutor());
    }

    GrokPrepper(final PluginSetting pluginSetting, final GrokCompiler grokCompiler, final ExecutorService executorService) {
        super(pluginSetting);
        this.grokPrepperConfig = GrokPrepperConfig.buildConfig(pluginSetting);
        this.keysToOverwrite = new HashSet<>(grokPrepperConfig.getkeysToOverwrite());
        this.grokCompiler = grokCompiler;
        this.fieldToGrok = new LinkedHashMap<>();
        this.executorService = executorService;

        grokProcessingMatchSuccessCounter = pluginMetrics.counter(GROK_PROCESSING_MATCH_SUCCESS);
        grokProcessingMatchFailureCounter = pluginMetrics.counter(GROK_PROCESSING_MATCH_FAILURE);
        grokProcessingErrorsCounter = pluginMetrics.counter(GROK_PROCESSING_ERRORS);
        grokProcessingTimeoutsCounter = pluginMetrics.counter(GROK_PROCESSING_TIMEOUTS);
        grokProcessingTime = pluginMetrics.timer(GROK_PROCESSING_TIME);

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
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        final List<Record<Event>> recordsOut = new LinkedList<>();

        for (final Record<Event> record : records) {
            try {
                final Event event = record.getData();

                if (grokPrepperConfig.getTimeoutMillis() == 0) {
                    grokProcessingTime.record(() -> matchAndMerge(event));
                } else {
                    runWithTimeout(() -> grokProcessingTime.record(() -> matchAndMerge(event)));
                }

                final Record<Event> grokkedRecord = new Record<>(event, record.getMetadata());
                recordsOut.add(grokkedRecord);
            } catch (TimeoutException e) {
                LOG.error("Matching on record [{}] took longer than [{}] and timed out", record.getData(), grokPrepperConfig.getTimeoutMillis());
                recordsOut.add(record);
                grokProcessingTimeoutsCounter.increment();
            } catch (ExecutionException e) {
                LOG.error("An exception occurred while matching on record [{}]", record.getData(), e);
                recordsOut.add(record);
                grokProcessingErrorsCounter.increment();
            } catch (InterruptedException e) {
                LOG.error("Matching on record [{}] was interrupted", record.getData(), e);
                recordsOut.add(record);
                grokProcessingErrorsCounter.increment();
            } catch (RuntimeException e) {
                LOG.error("Unknown exception occurred when matching record [{}]", record.getData(), e);
                recordsOut.add(record);
                grokProcessingErrorsCounter.increment();
            }
         }
        return recordsOut;
    }

    @Override
    public void prepareForShutdown() {
        executorService.shutdown();
    }

    @Override
    public boolean isReadyForShutdown() {
        try {
            if (executorService.awaitTermination(300, TimeUnit.MILLISECONDS)) {
                LOG.info("Successfully waited for running task to terminate");
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for running task to terminate", e);
        }
        return true;
    }

    @Override
    public void shutdown() {
        executorService.shutdownNow();
    }

    private void registerPatterns() {
        grokCompiler.registerDefaultPatterns();
        grokCompiler.register(grokPrepperConfig.getPatternDefinitions());
        registerPatternsDirectories();
    }

    private void registerPatternsDirectories() {
        for (final String directory : grokPrepperConfig.getPatternsDirectories()) {
            final Path path = FileSystems.getDefault().getPath(directory);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, grokPrepperConfig.getPatternsFilesGlob())) {
                for (final Path patternFile : stream) {
                    registerPatternsForFile(patternFile.toFile());
                }
            }
            catch (PatternSyntaxException e) {
                LOG.error("Glob pattern {} is invalid", grokPrepperConfig.getPatternsFilesGlob());
            } catch (NotDirectoryException e) {
                LOG.error("{} is not a directory", directory, e);
            } catch (IOException e) {
                LOG.error("Error getting directory {}", directory, e);
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

    private void matchAndMerge(final Event event) {
        final Map<String, Object> grokkedCaptures = new HashMap<>();

        for (final Map.Entry<String, List<Grok>> entry : fieldToGrok.entrySet()) {
            for (final Grok grok : entry.getValue()) {
                final String field = event.get(entry.getKey(), String.class);
                if (!field.isEmpty()) {
                    final Match match = grok.match(field);
                    match.setKeepEmptyCaptures(grokPrepperConfig.isKeepEmptyCaptures());

                    final Map<String, Object> captures = match.capture();
                    mergeCaptures(grokkedCaptures, captures);

                    if (shouldBreakOnMatch(grokkedCaptures)) {
                        break;
                    }
                }
            }
            if (shouldBreakOnMatch(grokkedCaptures)) {
                break;
            }
        }

        if (grokPrepperConfig.getTargetKey() != null) {
            event.put(grokPrepperConfig.getTargetKey(), grokkedCaptures);
        } else {
            mergeCaptures(event, grokkedCaptures);
        }

        if (grokkedCaptures.isEmpty()) {
            grokProcessingMatchFailureCounter.increment();
        } else {
            grokProcessingMatchSuccessCounter.increment();
        }
    }

    private void mergeCaptures(final Map<String, Object> original, final Map<String, Object> updates) {
        for (final Map.Entry<String, Object> updateEntry : updates.entrySet()) {
            if (!(original.containsKey(updateEntry.getKey())) || keysToOverwrite.contains(updateEntry.getKey())) {
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

    private void mergeCaptures(final Event event, final Map<String, Object> updates) {
        for (final Map.Entry<String, Object> updateEntry : updates.entrySet()) {

            if (!(event.containsKey(updateEntry.getKey())) || keysToOverwrite.contains(updateEntry.getKey())) {
                event.put(updateEntry.getKey(), updateEntry.getValue());
                continue;
            }

            if (event.isValueAList(updateEntry.getKey())) {
                final List<Object> fieldList = event.getList(updateEntry.getKey(), Object.class);
                mergeValueWithValues(updateEntry.getValue(), fieldList);
                event.put(updateEntry.getKey(), fieldList);
            } else {
                final Object fieldObject = event.get(updateEntry.getKey(), Object.class);
                final List<Object> values = new ArrayList<>(Collections.singletonList(fieldObject));
                mergeValueWithValues(updateEntry.getValue(), values);
                event.put(updateEntry.getKey(), values);
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

    private boolean shouldBreakOnMatch(final Map<String, Object> captures) {
        return captures.size() > 0 && grokPrepperConfig.isBreakOnMatch();
    }

    private void runWithTimeout(final Runnable runnable) throws TimeoutException, ExecutionException, InterruptedException {
        Future<?> task = executorService.submit(runnable);
        task.get(grokPrepperConfig.getTimeoutMillis(), TimeUnit.MILLISECONDS);
    }
}