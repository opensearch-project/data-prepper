/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.processor.Processor;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DeprecatedPluginDetectorTest {
    @Mock
    private DefinedPlugin definedPlugin;
    private TestLogAppender testAppender;

    @BeforeEach
    void setUp() {
        final Logger logger = (Logger) LoggerFactory.getLogger(DeprecatedPluginDetector.class);

        testAppender = new TestLogAppender();
        testAppender.start();
        logger.addAppender(testAppender);
    }

    private DeprecatedPluginDetector createObjectUnderTest() {
        return new DeprecatedPluginDetector();
    }

    @Test
    void accept_on_plugin_without_deprecated_name_does_not_log() {
        when(definedPlugin.getPluginClass()).thenReturn(PluginWithoutDeprecatedName.class);
        createObjectUnderTest().accept(definedPlugin);

        assertThat(testAppender.getLoggedEvents(), empty());
    }

    @Test
    void accept_on_plugin_with_deprecated_name_does_not_log_if_new_name_is_used() {
        when(definedPlugin.getPluginClass()).thenReturn(PluginWithDeprecatedName.class);
        when(definedPlugin.getPluginName()).thenReturn("test_for_deprecated_detection");
        createObjectUnderTest().accept(definedPlugin);

        assertThat(testAppender.getLoggedEvents(), empty());
    }

    @Test
    void accept_on_plugin_with_deprecated_name_logs_if_deprecated_name_is_used() {
        when(definedPlugin.getPluginClass()).thenReturn(PluginWithDeprecatedName.class);
        when(definedPlugin.getPluginName()).thenReturn("test_for_deprecated_detection_deprecated_name");
        createObjectUnderTest().accept(definedPlugin);

        assertThat(testAppender.getLoggedEvents().stream()
                .anyMatch(event -> event.getFormattedMessage().contains("Plugin name 'test_for_deprecated_detection_deprecated_name' is deprecated and will be removed in the next major release. Consider using the updated plugin name 'test_for_deprecated_detection'.")),
            equalTo(true));
    }

    @DataPrepperPlugin(name = "test_for_deprecated_detection", pluginType = Processor.class)
    private static class PluginWithoutDeprecatedName {
    }

    @DataPrepperPlugin(name = "test_for_deprecated_detection", pluginType = Processor.class, deprecatedName = "test_for_deprecated_detection_deprecated_name")
    private static class PluginWithDeprecatedName {
    }

    public static class TestLogAppender extends AppenderBase<ILoggingEvent> {
        private final List<ILoggingEvent> events = new ArrayList<>();

        @Override
        protected void append(final ILoggingEvent eventObject) {
            events.add(eventObject);
        }

        public List<ILoggingEvent> getLoggedEvents() {
            return Collections.unmodifiableList(events);
        }
    }
}