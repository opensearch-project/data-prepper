/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.sourcecoordination;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.parser.model.SourceCoordinationConfig;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;

import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class SourceCoordinatorFactoryTest {

    @Mock
    private SourceCoordinationConfig sourceCoordinationConfig;
    @Mock
    private PluginFactory pluginFactory;

    private SourceCoordinatorFactory createObjectUnderTest() {
        return new SourceCoordinatorFactory(sourceCoordinationConfig, pluginFactory);
    }

    @Test
    void provideSourceCoordinatorWithNullSourceCoordinationConfig_returns_null() {
        final SourceCoordinator<String> sourceCoordinator = createObjectUnderTest().provideSourceCoordinator(String.class, UUID.randomUUID().toString());

        assertThat(sourceCoordinator, nullValue());
    }

    @Test
    void provideEnhancedSourceCoordinatorWithNullSourceCoordinationConfig_returns_null() {
        final EnhancedSourceCoordinator enhancedSourceCoordinator = createObjectUnderTest().provideEnhancedSourceCoordinator(
                (item) -> new TestEnhancedSourcePartition(UUID.randomUUID().toString(), UUID.randomUUID().toString()), UUID.randomUUID().toString());

        assertThat(enhancedSourceCoordinator, nullValue());
    }

    @Test
    void provideSourceCoordinatorWithNullSourceCoordinationStoreConfig_returns_null() {
        given(sourceCoordinationConfig.getSourceCoordinationStoreConfig()).willReturn(null);

        final SourceCoordinator<String> sourceCoordinator = createObjectUnderTest().provideSourceCoordinator(String.class, UUID.randomUUID().toString());

        assertThat(sourceCoordinator, nullValue());
    }

    @Test
    void provideEnhancedSourceCoordinatorWithNullSourceCoordinationStoreConfig_returns_null() {
        given(sourceCoordinationConfig.getSourceCoordinationStoreConfig()).willReturn(null);

        final EnhancedSourceCoordinator sourceCoordinator = createObjectUnderTest().provideEnhancedSourceCoordinator(
                (item) -> new TestEnhancedSourcePartition(UUID.randomUUID().toString(), UUID.randomUUID().toString()), UUID.randomUUID().toString());

        assertThat(sourceCoordinator, nullValue());
    }



    @Test
    void provideSourceCoordinatorWith_no_name_no_store_name_returns_null() {
        final PluginSetting pluginSetting = mock(PluginSetting.class);
        given(sourceCoordinationConfig.getSourceCoordinationStoreConfig()).willReturn(pluginSetting);
        given(pluginSetting.getName()).willReturn(null);

        final SourceCoordinator<String> sourceCoordinator = createObjectUnderTest().provideSourceCoordinator(String.class, UUID.randomUUID().toString());

        assertThat(sourceCoordinator, nullValue());
    }

    @Test
    void provideEnhancedSourceCoordinatorWith_no_name_no_store_name_returns_null() {
        final PluginSetting pluginSetting = mock(PluginSetting.class);
        given(sourceCoordinationConfig.getSourceCoordinationStoreConfig()).willReturn(pluginSetting);
        given(pluginSetting.getName()).willReturn(null);

        final EnhancedSourceCoordinator sourceCoordinator = createObjectUnderTest().provideEnhancedSourceCoordinator(
                (item) -> new TestEnhancedSourcePartition(UUID.randomUUID().toString(), UUID.randomUUID().toString()), UUID.randomUUID().toString());
        assertThat(sourceCoordinator, nullValue());
    }

    @Test
    void provideSourceCoordinator_loads_expected_plugin_from_plugin_factory() {
        final String pluginName = UUID.randomUUID().toString();
        final PluginSetting pluginSetting = mock(PluginSetting.class);
        given(sourceCoordinationConfig.getSourceCoordinationStoreConfig()).willReturn(pluginSetting);
        given(pluginSetting.getName()).willReturn(pluginName);

        final SourceCoordinationStore expectedSourceCoordinationStore = mock(SourceCoordinationStore.class);

        given(pluginFactory.loadPlugin(SourceCoordinationStore.class, pluginSetting)).willReturn(expectedSourceCoordinationStore);

        final SourceCoordinator<String> actualSourceCoordinator = createObjectUnderTest().provideSourceCoordinator(String.class, UUID.randomUUID().toString());

        assertThat(actualSourceCoordinator, notNullValue());
    }

    @Test
    void provideEnhancedSourceCoordinator_loads_expected_plugin_from_plugin_factory() {
        final String pluginName = UUID.randomUUID().toString();
        final PluginSetting pluginSetting = mock(PluginSetting.class);
        given(sourceCoordinationConfig.getSourceCoordinationStoreConfig()).willReturn(pluginSetting);
        given(pluginSetting.getName()).willReturn(pluginName);

        final SourceCoordinationStore expectedSourceCoordinationStore = mock(SourceCoordinationStore.class);

        given(pluginFactory.loadPlugin(SourceCoordinationStore.class, pluginSetting)).willReturn(expectedSourceCoordinationStore);

        final EnhancedSourceCoordinator actualSourceCoordinator = createObjectUnderTest().provideEnhancedSourceCoordinator(
                (item) -> new TestEnhancedSourcePartition(UUID.randomUUID().toString(), UUID.randomUUID().toString()), UUID.randomUUID().toString());
        assertThat(actualSourceCoordinator, notNullValue());
    }

    public static class TestEnhancedSourcePartition extends EnhancedSourcePartition<String> {

        private final String partitionKey;
        private final String testPartitionProgressState;

        public TestEnhancedSourcePartition(final String partitionKey, final String partitionProgressState) {
            this.partitionKey = partitionKey;
            this.testPartitionProgressState = partitionProgressState;
        }

        @Override
        public String getPartitionType() {
            return "TEST";
        }

        @Override
        public String getPartitionKey() {
            return partitionKey;
        }

        @Override
        public Optional<String> getProgressState() {
            return Optional.of(testPartitionProgressState);
        }
    }
}
