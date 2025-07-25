/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationFileReader;
import org.opensearch.dataprepper.pipeline.parser.PipelinesDataflowModelParser;
import org.opensearch.dataprepper.pipeline.parser.model.PipelineConfiguration;
import org.opensearch.dataprepper.plugins.InMemorySinkAccessor;
import org.opensearch.dataprepper.plugins.InMemorySourceAccessor;
import org.opensearch.dataprepper.test.framework.DataPrepperTestRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.opensearch.dataprepper.test.framework.DataPrepperTestRunner.BASE_PATH;

@Disabled
class ProcessorSwapPipelineIT {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessorSwapPipelineIT.class);
    private static final String IN_MEMORY_IDENTIFIER = "ProcessorSwapPipelineIT";
    private static final String PIPELINE_CONFIGURATION_FOLDER_UNDER_TEST = "processor-swap";
    private static final String PIPELINE_NAME_IN_YAML = "processor-pipeline";
    private DataPrepperTestRunner dataPrepperTestRunner;
    private InMemorySourceAccessor inMemorySourceAccessor;
    private InMemorySinkAccessor inMemorySinkAccessor;
    private PluginFactory pluginFactory;

    @BeforeEach
    void setUp() {
        dataPrepperTestRunner = DataPrepperTestRunner.builder()
                .withPipelinesDirectoryOrFile(PIPELINE_CONFIGURATION_FOLDER_UNDER_TEST + "/source")
                .build();

        inMemorySourceAccessor = dataPrepperTestRunner.getInMemorySourceAccessor();
        inMemorySinkAccessor = dataPrepperTestRunner.getInMemorySinkAccessor();
        pluginFactory = dataPrepperTestRunner.getPluginFactory();
        dataPrepperTestRunner.start();
        LOG.info("Started test runner.");
    }

    @AfterEach
    void tearDown() {
        LOG.info("Test tear down. Stop the test runner.");
        dataPrepperTestRunner.stop();
    }

    @Test
    void run_with_single_record() {
        final String messageValue = UUID.randomUUID().toString();
        final Event event = JacksonEvent.fromMessage(messageValue);
        final Record<Event> eventRecord = new Record<>(event);

        LOG.info("Submitting a single record.");
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, Collections.singletonList(eventRecord));

        await().atMost(400, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER), not(empty())));

        final List<Record<Event>> records = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);

        assertThat(records.size(), equalTo(1));

        assertThat(records.get(0), notNullValue());
        assertThat(records.get(0).getData(), notNullValue());
        assertThat(records.get(0).getData().get("message", String.class), equalTo(messageValue));
        assertThat(records.get(0).getData().get("test1", String.class), equalTo("knownPrefix10"));
        assertThat(records.get(0).getData().get("test1_copy_original", String.class), equalTo("knownPrefix10"));

        // Dynamically swap the pipeline processors
        LOG.info("Swapping the pipeline processors");
        List<Processor> targetPipelineProcessors = getTargetPipelineProcessors();
        dataPrepperTestRunner.swapProcessors(PIPELINE_NAME_IN_YAML, targetPipelineProcessors);

        // Send one more event and assert against the updated processor behavior
        LOG.info("Submitting another single record.");
        final String updatedMessageValue = UUID.randomUUID().toString();
        final Event updatedEvent = JacksonEvent.fromMessage(updatedMessageValue);
        final Record<Event> updatedEventRecord = new Record<>(updatedEvent);
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, Collections.singletonList(updatedEventRecord));

        await().atMost(400, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER), not(empty())));

        final List<Record<Event>> updatedRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);

        LOG.info("Asserting the updated processor behavior");
        assertThat(updatedRecords.size(), equalTo(2));
        Event processedOutput = updatedRecords.get(1).getData();

        assertThat(processedOutput, notNullValue());
        assertThat(processedOutput, notNullValue());
        assertThat(processedOutput.get("message", String.class), equalTo(updatedMessageValue));
        assertThat(processedOutput.get("test1", String.class), equalTo("knownUpdatedPrefix10"));
        assertThat(processedOutput.get("test1_copy_updated", String.class), equalTo("knownUpdatedPrefix10"));

    }


    private List<Processor> getTargetPipelineProcessors() {
        // Create the target pipeline
        String targetPipelineFolderPath = BASE_PATH + "/pipeline/" + PIPELINE_CONFIGURATION_FOLDER_UNDER_TEST + "/target";
        PipelinesDataFlowModel targetPipelinesDataFlowModel =
                new PipelinesDataflowModelParser(
                        new PipelineConfigurationFileReader(targetPipelineFolderPath))
                        .parseConfiguration();

        PipelineModel pipelineModel = targetPipelinesDataFlowModel.getPipelines().get(PIPELINE_NAME_IN_YAML);
        PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(pipelineModel);
        List<Processor> processors = new ArrayList<>();
        for (PluginSetting pluginSetting : pipelineConfiguration.getProcessorPluginSettings()) {
            List<Processor> processorsList =
                    pluginFactory.loadPlugins(Processor.class, pluginSetting, (actualClass -> 1));
            processors.add(processorsList.get(0));
        }
        return processors;
    }
}
