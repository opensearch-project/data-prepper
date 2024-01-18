/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.aws.AwsSecretPlugin.PERIOD_IN_SECONDS;

@ExtendWith(MockitoExtension.class)
class AwsSecretPluginIT {
    private static String TEST_SECRET_CONFIG_ID = "testSecretConfig";
    @Mock
    private AwsSecretPluginConfig awsSecretPluginConfig;

    @Mock
    private AwsSecretManagerConfiguration awsSecretManagerConfiguration;

    @Mock
    private SecretsManagerClient secretsManagerClient;

    @Mock
    private GetSecretValueRequest getSecretValueRequest;

    @Mock
    private GetSecretValueResponse getSecretValueResponse;

    @Mock
    private ExtensionPoints extensionPoints;

    @Mock
    private ExtensionProvider.Context context;

    @Mock
    private ScheduledExecutorService scheduledExecutorService;

    @Mock
    private Runtime runtime;

    @Captor
    private ArgumentCaptor<Long> initialDelayCaptor;

    @Captor
    private ArgumentCaptor<Long> periodCaptor;

    private AwsSecretPlugin objectUnderTest;

    @Test
    void testInitializationWithNonNullConfig() {
        final Duration testInterval = Duration.ofHours(2);
        when(awsSecretPluginConfig.getAwsSecretManagerConfigurationMap()).thenReturn(
                Map.of(TEST_SECRET_CONFIG_ID, awsSecretManagerConfiguration));
        when(awsSecretManagerConfiguration.getRefreshInterval()).thenReturn(testInterval);
        when(awsSecretManagerConfiguration.createSecretManagerClient()).thenReturn(secretsManagerClient);
        when(awsSecretManagerConfiguration.createGetSecretValueRequest()).thenReturn(getSecretValueRequest);
        when(secretsManagerClient.getSecretValue(eq(getSecretValueRequest))).thenReturn(getSecretValueResponse);
        when(getSecretValueResponse.secretString()).thenReturn(UUID.randomUUID().toString());
        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class);
             final MockedStatic<Runtime> runtimeMockedStatic = mockStatic(Runtime.class)
        ) {
            executorsMockedStatic.when(Executors::newSingleThreadScheduledExecutor)
                    .thenReturn(scheduledExecutorService);
            runtimeMockedStatic.when(Runtime::getRuntime).thenReturn(runtime);
            objectUnderTest = new AwsSecretPlugin(awsSecretPluginConfig);
            objectUnderTest.apply(extensionPoints);
        }
        final ArgumentCaptor<ExtensionProvider> extensionProviderArgumentCaptor =
                ArgumentCaptor.forClass(ExtensionProvider.class);
        verify(extensionPoints, times(2)).addExtensionProvider(extensionProviderArgumentCaptor.capture());
        final List<ExtensionProvider> actualExtensionProviders = extensionProviderArgumentCaptor.getAllValues();
        assertThat(actualExtensionProviders.get(0), instanceOf(AwsSecretsPluginConfigValueTranslatorExtensionProvider.class));
        final Optional<PluginConfigValueTranslator> optionalPluginConfigValueTranslator =
                actualExtensionProviders.get(0).provideInstance(context);
        assertThat(optionalPluginConfigValueTranslator.isPresent(), is(true));
        assertThat(optionalPluginConfigValueTranslator.get(), instanceOf(AwsSecretsPluginConfigValueTranslator.class));
        assertThat(actualExtensionProviders.get(1), instanceOf(AwsSecretsPluginConfigPublisherExtensionProvider.class));
        final Optional<PluginConfigPublisher> optionalPluginConfigPublisher =
                actualExtensionProviders.get(1).provideInstance(context);
        assertThat(optionalPluginConfigPublisher.isPresent(), is(true));
        verify(scheduledExecutorService).scheduleAtFixedRate(
                any(), initialDelayCaptor.capture(), periodCaptor.capture(), eq(TimeUnit.SECONDS));
        assertThat(initialDelayCaptor.getValue() >= testInterval.toSeconds(), is(true));
        assertThat(periodCaptor.getValue(), equalTo(testInterval.toSeconds()));
        verify(runtime).addShutdownHook(any());
    }

    @Test
    void testInitializationWithNullConfig() {
        objectUnderTest = new AwsSecretPlugin(null);
        objectUnderTest.apply(extensionPoints);
        final ArgumentCaptor<ExtensionProvider> extensionProviderArgumentCaptor =
                ArgumentCaptor.forClass(ExtensionProvider.class);
        verify(extensionPoints, times(2)).addExtensionProvider(extensionProviderArgumentCaptor.capture());
        final List<ExtensionProvider> actualExtensionProviders = extensionProviderArgumentCaptor.getAllValues();
        assertThat(actualExtensionProviders.get(0), instanceOf(AwsSecretsPluginConfigValueTranslatorExtensionProvider.class));
        final Optional<PluginConfigValueTranslator> optionalPluginConfigValueTranslator =
                actualExtensionProviders.get(0).provideInstance(context);
        assertThat(optionalPluginConfigValueTranslator.isEmpty(), is(true));
        assertThat(actualExtensionProviders.get(1), instanceOf(AwsSecretsPluginConfigPublisherExtensionProvider.class));
        final Optional<PluginConfigPublisher> optionalPluginConfigPublisher =
                actualExtensionProviders.get(1).provideInstance(context);
        assertThat(optionalPluginConfigPublisher.isEmpty(), is(true));
    }

    @Test
    void testShutdownAwaitTerminationSuccess() throws InterruptedException {
        when(awsSecretPluginConfig.getAwsSecretManagerConfigurationMap()).thenReturn(
                Collections.emptyMap());
        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class);
             final MockedStatic<Runtime> runtimeMockedStatic = mockStatic(Runtime.class)
        ) {
            executorsMockedStatic.when(Executors::newSingleThreadScheduledExecutor)
                    .thenReturn(scheduledExecutorService);
            runtimeMockedStatic.when(Runtime::getRuntime).thenReturn(runtime);
            objectUnderTest = new AwsSecretPlugin(awsSecretPluginConfig);
        }
        when(scheduledExecutorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
        objectUnderTest.shutdown();

        verify(scheduledExecutorService).shutdown();
        verify(scheduledExecutorService).awaitTermination(
                eq(Integer.valueOf(PERIOD_IN_SECONDS).longValue()), eq(TimeUnit.SECONDS));
        verify(scheduledExecutorService, times(0)).shutdownNow();
    }

    @Test
    void testShutdownAwaitTerminationTimeout() throws InterruptedException {
        when(awsSecretPluginConfig.getAwsSecretManagerConfigurationMap()).thenReturn(
                Collections.emptyMap());
        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class);
             final MockedStatic<Runtime> runtimeMockedStatic = mockStatic(Runtime.class)
        ) {
            executorsMockedStatic.when(Executors::newSingleThreadScheduledExecutor)
                    .thenReturn(scheduledExecutorService);
            runtimeMockedStatic.when(Runtime::getRuntime).thenReturn(runtime);
            objectUnderTest = new AwsSecretPlugin(awsSecretPluginConfig);
        }
        when(scheduledExecutorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(false);
        objectUnderTest.shutdown();

        verify(scheduledExecutorService).shutdown();
        verify(scheduledExecutorService).awaitTermination(
                eq(Integer.valueOf(PERIOD_IN_SECONDS).longValue()), eq(TimeUnit.SECONDS));
        verify(scheduledExecutorService).shutdownNow();
    }

    @Test
    void testShutdownAwaitTerminationInterrupted() throws InterruptedException {
        when(awsSecretPluginConfig.getAwsSecretManagerConfigurationMap()).thenReturn(
                Collections.emptyMap());
        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class);
             final MockedStatic<Runtime> runtimeMockedStatic = mockStatic(Runtime.class)
        ) {
            executorsMockedStatic.when(Executors::newSingleThreadScheduledExecutor)
                    .thenReturn(scheduledExecutorService);
            runtimeMockedStatic.when(Runtime::getRuntime).thenReturn(runtime);
            objectUnderTest = new AwsSecretPlugin(awsSecretPluginConfig);
        }
        when(scheduledExecutorService.awaitTermination(anyLong(), any(TimeUnit.class)))
                .thenThrow(new InterruptedException());
        objectUnderTest.shutdown();

        verify(scheduledExecutorService).shutdown();
        verify(scheduledExecutorService).awaitTermination(
                eq(Integer.valueOf(PERIOD_IN_SECONDS).longValue()), eq(TimeUnit.SECONDS));
        verify(scheduledExecutorService).shutdownNow();
    }

    @Test
    void testShutdownWithNullScheduledExecutorService() {
        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class);
             final MockedStatic<Runtime> runtimeMockedStatic = mockStatic(Runtime.class)
        ) {
            executorsMockedStatic.when(Executors::newSingleThreadScheduledExecutor)
                    .thenReturn(scheduledExecutorService);
            runtimeMockedStatic.when(Runtime::getRuntime).thenReturn(runtime);
            objectUnderTest = new AwsSecretPlugin(null);
        }
        objectUnderTest.shutdown();
        verifyNoInteractions(scheduledExecutorService);
    }
}