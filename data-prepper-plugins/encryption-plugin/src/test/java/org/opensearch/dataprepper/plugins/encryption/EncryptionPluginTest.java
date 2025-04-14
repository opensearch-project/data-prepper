/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.encryption.EncryptionEngine;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EncryptionPluginTest {
    private static final String TEST_ENCRYPTION_ID = UUID.randomUUID().toString();

    @Mock
    private ExtensionPoints extensionPoints;
    @Mock
    private ExtensionProvider.Context context;
    @Mock
    private EncryptionPluginConfig encryptionPluginConfig;
    @Mock
    private EncryptionEngineConfiguration encryptionEngineConfiguration;
    @Mock
    private EncryptedDataKeySupplierFactory encryptedDataKeySupplierFactory;
    @Mock
    private EncryptedDataKeySupplier encryptedDataKeySupplier;
    @Mock
    private EncryptionEngineFactory encryptionEngineFactory;
    @Mock
    private EncryptionEngine encryptionEngine;
    @Mock
    private EncryptionRotationHandlerFactory encryptionRotationHandlerFactory;
    @Mock
    private EncryptionRotationHandler encryptionRotationHandler;
    @Mock
    private DefaultEncryptionHttpHandler defaultEncryptionHttpHandler;
    @Mock
    private ScheduledExecutorService scheduledExecutorService;

    private EncryptionPlugin objectUnderTest;

    @Test
    void testInitializationWithRotationEnabled() {
        when(encryptionPluginConfig.getEncryptionConfigurationMap()).thenReturn(
                Map.of(TEST_ENCRYPTION_ID, encryptionEngineConfiguration));
        when(encryptedDataKeySupplierFactory.createEncryptedDataKeySupplier(encryptionEngineConfiguration))
                .thenReturn(encryptedDataKeySupplier);
        when(encryptionEngineFactory.createEncryptionEngine(encryptionEngineConfiguration, encryptedDataKeySupplier))
                .thenReturn(encryptionEngine);
        when(encryptionRotationHandlerFactory.createEncryptionRotationHandler(
                eq(TEST_ENCRYPTION_ID), any(EncryptionEngineConfiguration.class)))
                .thenReturn(encryptionRotationHandler);
        when(encryptionEngineConfiguration.rotationEnabled()).thenReturn(true);
        when(encryptionEngineConfiguration.getRotationInterval()).thenReturn(Duration.ofMillis(1000));
        try (final MockedStatic<EncryptedDataKeySupplierFactory> encryptedDataKeySupplierFactoryMockedStatic =
                     mockStatic(EncryptedDataKeySupplierFactory.class);
             final MockedStatic<EncryptionEngineFactory> encryptionEngineFactoryMockedStatic =
                     mockStatic(EncryptionEngineFactory.class);
             final MockedStatic<EncryptionRotationHandlerFactory> encryptionRotationHandlerFactoryMockedStatic =
                     mockStatic(EncryptionRotationHandlerFactory.class);
             final MockedStatic<DefaultEncryptionHttpHandler> defaultEncryptionHttpHandlerMockedStatic =
                     mockStatic(DefaultEncryptionHttpHandler.class);
        ) {
            encryptedDataKeySupplierFactoryMockedStatic.when(EncryptedDataKeySupplierFactory::create)
                    .thenReturn(encryptedDataKeySupplierFactory);
            encryptionEngineFactoryMockedStatic.when(() -> EncryptionEngineFactory.create(any(KeyProviderFactory.class)))
                    .thenReturn(encryptionEngineFactory);
            encryptionRotationHandlerFactoryMockedStatic.when(() -> EncryptionRotationHandlerFactory.create(
                    any(PluginMetrics.class), any(EncryptedDataKeyWriterFactory.class)))
                    .thenReturn(encryptionRotationHandlerFactory);
            defaultEncryptionHttpHandlerMockedStatic.when(() -> DefaultEncryptionHttpHandler.create(anySet()))
                    .thenReturn(defaultEncryptionHttpHandler);
            objectUnderTest = new EncryptionPlugin(encryptionPluginConfig);
            objectUnderTest.apply(extensionPoints);
            final ArgumentCaptor<ExtensionProvider> extensionProviderArgumentCaptor =
                    ArgumentCaptor.forClass(ExtensionProvider.class);
            verify(extensionPoints, times(2))
                    .addExtensionProvider(extensionProviderArgumentCaptor.capture());
            final List<ExtensionProvider> actualExtensionProviders = extensionProviderArgumentCaptor.getAllValues();
            assertThat(actualExtensionProviders.get(0), instanceOf(EncryptionSupplierExtensionProvider.class));
            final Optional<EncryptionSupplier> optionalEncryptionSupplier =
                    actualExtensionProviders.get(0).provideInstance(context);
            assertThat(optionalEncryptionSupplier.isPresent(), is(true));
            final EncryptionSupplier encryptionSupplier = optionalEncryptionSupplier.get();
            assertThat(encryptionSupplier.getEncryptionEngineMap().isEmpty(), is(false));
            assertThat(encryptionSupplier.getEncryptedDataKeySupplierMap().isEmpty(), is(false));
            assertThat(actualExtensionProviders.get(1), instanceOf(EncryptionHttpHandlerExtensionProvider.class));
            final Optional<EncryptionHttpHandler> optionalEncryptionHttpHandler = actualExtensionProviders.get(1)
                    .provideInstance(context);
            assertThat(optionalEncryptionHttpHandler.isPresent(), is(true));
            assertThat(optionalEncryptionHttpHandler.get(), is(defaultEncryptionHttpHandler));
            final ArgumentCaptor<Set<EncryptionRotationHandler>> encryptionRotationHandlersArgumentCaptor =
                    ArgumentCaptor.forClass(Set.class);
            defaultEncryptionHttpHandlerMockedStatic.verify(
                    () -> DefaultEncryptionHttpHandler.create(encryptionRotationHandlersArgumentCaptor.capture()));
            final Set<EncryptionRotationHandler> encryptionRotationHandlers =
                    encryptionRotationHandlersArgumentCaptor.getValue();
            assertThat(encryptionRotationHandlers.isEmpty(), is(false));
            assertThat(encryptionRotationHandlers.contains(encryptionRotationHandler), is(true));
        }
    }

    @Test
    void testInitializationWithRotationDisabled() {
        when(encryptionPluginConfig.getEncryptionConfigurationMap()).thenReturn(
                Map.of(TEST_ENCRYPTION_ID, encryptionEngineConfiguration));
        when(encryptedDataKeySupplierFactory.createEncryptedDataKeySupplier(encryptionEngineConfiguration))
                .thenReturn(encryptedDataKeySupplier);
        when(encryptionEngineFactory.createEncryptionEngine(encryptionEngineConfiguration, encryptedDataKeySupplier))
                .thenReturn(encryptionEngine);
        when(encryptionEngineConfiguration.rotationEnabled()).thenReturn(false);
        try (final MockedStatic<EncryptedDataKeySupplierFactory> encryptedDataKeySupplierFactoryMockedStatic =
                     mockStatic(EncryptedDataKeySupplierFactory.class);
             final MockedStatic<EncryptionEngineFactory> encryptionEngineFactoryMockedStatic =
                     mockStatic(EncryptionEngineFactory.class);
             final MockedStatic<EncryptionRotationHandlerFactory> encryptionRotationHandlerFactoryMockedStatic =
                     mockStatic(EncryptionRotationHandlerFactory.class);
             final MockedStatic<DefaultEncryptionHttpHandler> defaultEncryptionHttpHandlerMockedStatic =
                     mockStatic(DefaultEncryptionHttpHandler.class)
        ) {
            encryptedDataKeySupplierFactoryMockedStatic.when(EncryptedDataKeySupplierFactory::create)
                    .thenReturn(encryptedDataKeySupplierFactory);
            encryptionEngineFactoryMockedStatic.when(() -> EncryptionEngineFactory.create(any(KeyProviderFactory.class)))
                    .thenReturn(encryptionEngineFactory);
            encryptionRotationHandlerFactoryMockedStatic.when(() -> EncryptionRotationHandlerFactory.create(
                            any(PluginMetrics.class), any(EncryptedDataKeyWriterFactory.class)))
                    .thenReturn(encryptionRotationHandlerFactory);
            defaultEncryptionHttpHandlerMockedStatic.when(() -> DefaultEncryptionHttpHandler.create(anySet()))
                    .thenReturn(defaultEncryptionHttpHandler);
            objectUnderTest = new EncryptionPlugin(encryptionPluginConfig);
            objectUnderTest.apply(extensionPoints);
            final ArgumentCaptor<ExtensionProvider> extensionProviderArgumentCaptor =
                    ArgumentCaptor.forClass(ExtensionProvider.class);
            verify(extensionPoints, times(2))
                    .addExtensionProvider(extensionProviderArgumentCaptor.capture());
            final List<ExtensionProvider> actualExtensionProviders = extensionProviderArgumentCaptor.getAllValues();
            assertThat(actualExtensionProviders.get(0), instanceOf(EncryptionSupplierExtensionProvider.class));
            final Optional<EncryptionSupplier> optionalEncryptionSupplier =
                    actualExtensionProviders.get(0).provideInstance(context);
            assertThat(optionalEncryptionSupplier.isPresent(), is(true));
            final EncryptionSupplier encryptionSupplier = optionalEncryptionSupplier.get();
            assertThat(encryptionSupplier.getEncryptionEngineMap().isEmpty(), is(false));
            assertThat(encryptionSupplier.getEncryptedDataKeySupplierMap().isEmpty(), is(false));
            assertThat(actualExtensionProviders.get(1), instanceOf(EncryptionHttpHandlerExtensionProvider.class));
            final Optional<EncryptionHttpHandler> optionalEncryptionHttpHandler = actualExtensionProviders.get(1)
                    .provideInstance(context);
            assertThat(optionalEncryptionHttpHandler.isPresent(), is(true));
            assertThat(optionalEncryptionHttpHandler.get(), is(defaultEncryptionHttpHandler));
            final ArgumentCaptor<Set<EncryptionRotationHandler>> encryptionRotationHandlersArgumentCaptor =
                    ArgumentCaptor.forClass(Set.class);
            defaultEncryptionHttpHandlerMockedStatic.verify(
                    () -> DefaultEncryptionHttpHandler.create(encryptionRotationHandlersArgumentCaptor.capture()));
            final Set<EncryptionRotationHandler> encryptionRotationHandlers =
                    encryptionRotationHandlersArgumentCaptor.getValue();
            assertThat(encryptionRotationHandlers.isEmpty(), is(true));
        }
    }

    @Test
    void testInitializationWithNullConfig() {
        try (final MockedStatic<DefaultEncryptionHttpHandler> defaultEncryptionHttpHandlerMockedStatic =
                     mockStatic(DefaultEncryptionHttpHandler.class)) {
            defaultEncryptionHttpHandlerMockedStatic.when(() -> DefaultEncryptionHttpHandler.create(anySet()))
                    .thenReturn(defaultEncryptionHttpHandler);
            objectUnderTest = new EncryptionPlugin(null);
            objectUnderTest.apply(extensionPoints);
            final ArgumentCaptor<ExtensionProvider> extensionProviderArgumentCaptor =
                    ArgumentCaptor.forClass(ExtensionProvider.class);
            verify(extensionPoints, times(2))
                    .addExtensionProvider(extensionProviderArgumentCaptor.capture());
            final List<ExtensionProvider> actualExtensionProviders = extensionProviderArgumentCaptor.getAllValues();
            assertThat(actualExtensionProviders.get(0), instanceOf(EncryptionSupplierExtensionProvider.class));
            final Optional<EncryptionSupplier> optionalEncryptionSupplier =
                    actualExtensionProviders.get(0).provideInstance(context);
            assertThat(optionalEncryptionSupplier.isPresent(), is(true));
            final EncryptionSupplier encryptionSupplier = optionalEncryptionSupplier.get();
            assertThat(encryptionSupplier.getEncryptionEngineMap(), equalTo(Collections.emptyMap()));
            assertThat(encryptionSupplier.getEncryptedDataKeySupplierMap(), equalTo(Collections.emptyMap()));
            assertThat(actualExtensionProviders.get(1), instanceOf(EncryptionHttpHandlerExtensionProvider.class));
            final Optional<EncryptionHttpHandler> optionalEncryptionHttpHandler = actualExtensionProviders.get(1)
                    .provideInstance(context);
            assertThat(optionalEncryptionHttpHandler.isPresent(), is(true));
            final ArgumentCaptor<Set<EncryptionRotationHandler>> encryptionRotationHandlersArgumentCaptor =
                    ArgumentCaptor.forClass(Set.class);
            defaultEncryptionHttpHandlerMockedStatic.verify(
                    () -> DefaultEncryptionHttpHandler.create(encryptionRotationHandlersArgumentCaptor.capture()));
            final Set<EncryptionRotationHandler> encryptionRotationHandlers =
                    encryptionRotationHandlersArgumentCaptor.getValue();
            assertThat(encryptionRotationHandlers.isEmpty(), is(true));
        }
    }
}