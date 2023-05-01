/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.grok;


import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.SingleThread;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;


@SingleThread
@DataPrepperPlugin(name = "grok", pluginType = Processor.class)
public class GrokProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    static final long EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT = 300L;

    private static final Logger LOG = LoggerFactory.getLogger(GrokProcessor.class);

    private static final String DATA_PREPPER_GROK_PATTERNS_FILE = "grok-patterns/patterns";

    static final String GROK_PROCESSING_MATCH = "grokProcessingMatch";
    static final String GROK_PROCESSING_MISMATCH = "grokProcessingMismatch";
    static final String GROK_PROCESSING_ERRORS = "grokProcessingErrors";
    static final String GROK_PROCESSING_TIMEOUTS = "grokProcessingTimeouts";
    static final String GROK_PROCESSING_TIME = "grokProcessingTime";

    private final Counter grokProcessingMismatchCounter;
    private final Counter grokProcessingMatchCounter;
    private final Counter grokProcessingErrorsCounter;
    private final Counter grokProcessingTimeoutsCounter;
    private final Timer grokProcessingTime;

    private final GrokCompiler grokCompiler;
    private final Map<String, List<Grok>> fieldToGrok;
    private final GrokProcessorConfig grokProcessorConfig;
    private final Set<String> keysToOverwrite;
    private final ExecutorService executorService;

    private final ExpressionEvaluator<Boolean> expressionEvaluator;

    @DataPrepperPluginConstructor
    public GrokProcessor(final PluginSetting pluginSetting, final ExpressionEvaluator<Boolean> expressionEvaluator) {
        this(pluginSetting, GrokCompiler.newInstance(), Executors.newSingleThreadExecutor(), expressionEvaluator);
    }

    GrokProcessor(final PluginSetting pluginSetting, final GrokCompiler grokCompiler, final ExecutorService executorService, final ExpressionEvaluator<Boolean> expressionEvaluator) {
        super(pluginSetting);
        this.grokProcessorConfig = GrokProcessorConfig.buildConfig(pluginSetting);
        this.keysToOverwrite = new HashSet<>(grokProcessorConfig.getkeysToOverwrite());
        this.grokCompiler = grokCompiler;
        this.fieldToGrok = new LinkedHashMap<>();
        this.executorService = executorService;
        this.expressionEvaluator = expressionEvaluator;

        grokProcessingMatchCounter = pluginMetrics.counter(GROK_PROCESSING_MATCH);
        grokProcessingMismatchCounter = pluginMetrics.counter(GROK_PROCESSING_MISMATCH);
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
        for (final Record<Event> record : records) {
            try {
                final Event event = record.getData();

                if (Objects.nonNull(grokProcessorConfig.getGrokWhen()) && !expressionEvaluator.evaluate(grokProcessorConfig.getGrokWhen(), event)) {
                    continue;
                }

                if (grokProcessorConfig.getTimeoutMillis() == 0) {
                    grokProcessingTime.record(() -> matchAndMerge(event));
                } else {
                    runWithTimeout(() -> grokProcessingTime.record(() -> matchAndMerge(event)));
                }

            } catch (TimeoutException e) {
                LOG.error(EVENT, "Matching on record [{}] took longer than [{}] and timed out", record.getData(), grokProcessorConfig.getTimeoutMillis());
                grokProcessingTimeoutsCounter.increment();
            } catch (ExecutionException e) {
                LOG.error(EVENT, "An exception occurred while matching on record [{}]", record.getData(), e);
                grokProcessingErrorsCounter.increment();
            } catch (InterruptedException e) {
                LOG.error(EVENT, "Matching on record [{}] was interrupted", record.getData(), e);
                grokProcessingErrorsCounter.increment();
            } catch (RuntimeException e) {
                LOG.error(EVENT, "Unknown exception occurred when matching record [{}]", record.getData(), e);
                grokProcessingErrorsCounter.increment();
            }
         }
        return records;
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
        executorService.shutdown();
        try {
            if (executorService.awaitTermination(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
                LOG.info("Successfully waited for running task to terminate");
            } else {
                LOG.warn("Running task did not terminate in time, forcing termination");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for running task to terminate", e);
            executorService.shutdownNow();
        }
    }

    private void registerPatterns() {
        grokCompiler.registerDefaultPatterns();
        registerBuiltInDataPrepperGrokPatterns();
        grokCompiler.register(grokProcessorConfig.getPatternDefinitions());
        registerPatternsDirectories();
    }

    private void registerBuiltInDataPrepperGrokPatterns() {
        try (
                final InputStream directoryStream = getClass().getClassLoader().getResourceAsStream(DATA_PREPPER_GROK_PATTERNS_FILE);
        ) {
            grokCompiler.register(directoryStream);
        } catch (final Exception e) {
            LOG.error("An exception occurred while initializing built in grok patterns for Data Prepper", e);
        }
    }

    private void registerPatternsDirectories() {
        for (final String directory : grokProcessorConfig.getPatternsDirectories()) {
            final Path path = FileSystems.getDefault().getPath(directory);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, grokProcessorConfig.getPatternsFilesGlob())) {
                for (final Path patternFile : stream) {
                    registerPatternsForFile(patternFile.toFile());
                }
            }
            catch (PatternSyntaxException e) {
                LOG.error("Glob pattern {} is invalid", grokProcessorConfig.getPatternsFilesGlob());
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
        for (final Map.Entry<String, List<String>> entry : grokProcessorConfig.getMatch().entrySet()) {
            fieldToGrok.put(entry.getKey(), entry.getValue()
                            .stream()
                            .map(item -> grokCompiler.compile(item, grokProcessorConfig.isNamedCapturesOnly()))
                            .collect(Collectors.toList()));
        }
    }

    private void matchAndMerge(final Event event) {
        final Map<String, Object> grokkedCaptures = new HashMap<>();

        for (final Map.Entry<String, List<Grok>> entry : fieldToGrok.entrySet()) {
            for (final Grok grok : entry.getValue()) {
                final String value = event.get(entry.getKey(), String.class);
                if (value != null && !value.isEmpty()) {
                    final Match match = grok.match(value);
                    match.setKeepEmptyCaptures(grokProcessorConfig.isKeepEmptyCaptures());

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

        if (grokProcessorConfig.getTargetKey() != null) {
            event.put(grokProcessorConfig.getTargetKey(), grokkedCaptures);
        } else {
            mergeCaptures(event, grokkedCaptures);
        }

        if (grokkedCaptures.isEmpty()) {
            grokProcessingMismatchCounter.increment();
        } else {
            grokProcessingMatchCounter.increment();
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
                final List<Object> values = event.getList(updateEntry.getKey(), Object.class);
                mergeValueWithValues(updateEntry.getValue(), values);
                event.put(updateEntry.getKey(), values);
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
        return captures.size() > 0 && grokProcessorConfig.isBreakOnMatch();
    }

    private void runWithTimeout(final Runnable runnable) throws TimeoutException, ExecutionException, InterruptedException {
        Future<?> task = executorService.submit(runnable);
        task.get(grokProcessorConfig.getTimeoutMillis(), TimeUnit.MILLISECONDS);
    }
}
