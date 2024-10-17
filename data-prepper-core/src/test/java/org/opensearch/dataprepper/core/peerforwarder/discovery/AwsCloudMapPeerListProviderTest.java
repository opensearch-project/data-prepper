/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.discovery;

import com.linecorp.armeria.client.Endpoint;
import com.linecorp.armeria.client.retry.Backoff;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.opensearch.dataprepper.core.peerforwarder.discovery.AwsCloudMapPeerListProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import software.amazon.awssdk.services.servicediscovery.ServiceDiscoveryAsyncClient;
import software.amazon.awssdk.services.servicediscovery.model.DiscoverInstancesRequest;
import software.amazon.awssdk.services.servicediscovery.model.DiscoverInstancesResponse;
import software.amazon.awssdk.services.servicediscovery.model.HttpInstanceSummary;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

class AwsCloudMapPeerListProviderTest {

    public static final int WAIT_TIME_MULTIPLIER_MILLIS = 30000;
    private ServiceDiscoveryAsyncClient awsServiceDiscovery;
    private String namespaceName;
    private String serviceName;
    private Map<String, String> queryParameters;
    private Duration timeToRefresh;
    private Backoff backoff;
    private PluginMetrics pluginMetrics;
    private List<AwsCloudMapPeerListProvider> objectsToClose;

    @BeforeEach
    void setUp() {
        awsServiceDiscovery = mock(ServiceDiscoveryAsyncClient.class);
        namespaceName = RandomStringUtils.randomAlphabetic(10);
        serviceName = RandomStringUtils.randomAlphabetic(10);
        queryParameters = generateRandomStringMap();

        timeToRefresh = Duration.ofMillis(200);
        backoff = mock(Backoff.class);
        pluginMetrics = mock(PluginMetrics.class);

        objectsToClose = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        objectsToClose.forEach(AwsCloudMapPeerListProvider::close);
    }

    private AwsCloudMapPeerListProvider createObjectUnderTest() {
        final AwsCloudMapPeerListProvider objectUnderTest =
                new AwsCloudMapPeerListProvider(awsServiceDiscovery, namespaceName, serviceName, queryParameters, timeToRefresh, backoff, pluginMetrics);
        objectsToClose.add(objectUnderTest);
        return objectUnderTest;
    }

    @Test
    void constructor_throws_with_null_AWSServiceDiscovery() {
        awsServiceDiscovery = null;

        assertThrows(NullPointerException.class,
                this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_with_null_Namespace() {
        namespaceName = null;

        assertThrows(NullPointerException.class,
                this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_with_null_ServiceName() {
        serviceName = null;

        assertThrows(NullPointerException.class,
                this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_with_null_QueryParameters() {
        queryParameters = null;

        assertThrows(NullPointerException.class,
                this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_with_null_Backoff() {
        backoff = null;

        assertThrows(NullPointerException.class,
                this::createObjectUnderTest);
    }

    @ParameterizedTest
    @ValueSource(ints = {Integer.MIN_VALUE, -10, -1, 0})
    void constructor_throws_with_non_positive_timeToRefreshSeconds(final int badTimeToRefresh) {
        timeToRefresh = Duration.ofSeconds(badTimeToRefresh);

        assertThrows(IllegalArgumentException.class,
                this::createObjectUnderTest);
    }

    @Test
    void constructor_should_DiscoverInstances_with_correct_request() {
        createObjectUnderTest();

        waitUntilDiscoverInstancesCalledAtLeastOnce();

        final ArgumentCaptor<DiscoverInstancesRequest> requestArgumentCaptor =
                ArgumentCaptor.forClass(DiscoverInstancesRequest.class);

        then(awsServiceDiscovery)
                .should()
                .discoverInstances(requestArgumentCaptor.capture());

        final DiscoverInstancesRequest actualRequest = requestArgumentCaptor.getValue();

        assertThat(actualRequest.namespaceName(), equalTo(namespaceName));
        assertThat(actualRequest.serviceName(), equalTo(serviceName));
        assertThat(actualRequest.queryParameters(), equalTo(queryParameters));
        assertThat(actualRequest.healthStatusAsString(), nullValue());
    }

    @Test
    void getPeerList_is_empty_before_populated() {
        final AwsCloudMapPeerListProvider objectUnderTest = createObjectUnderTest();

        waitUntilDiscoverInstancesCalledAtLeastOnce();

        final List<String> peerList = objectUnderTest.getPeerList();

        assertThat(peerList, notNullValue());
        assertThat(peerList.size(), equalTo(0));
    }

    @Nested
    class WithDiscoverInstances {

        private DiscoverInstancesResponse discoverInstancesResponse;

        @BeforeEach
        void setUp() {
            discoverInstancesResponse = mock(DiscoverInstancesResponse.class);

            final CompletableFuture<DiscoverInstancesResponse> discoverFuture =
                    CompletableFuture.completedFuture(discoverInstancesResponse);

            given(awsServiceDiscovery.discoverInstances(any(DiscoverInstancesRequest.class)))
                    .willReturn(discoverFuture);
        }

        @Test
        void getPeerList_returns_empty_when_DiscoverInstances_has_no_instances() {
            given(discoverInstancesResponse.instances()).willReturn(Collections.emptyList());

            final AwsCloudMapPeerListProvider objectUnderTest = createObjectUnderTest();

            waitUntilDiscoverInstancesCalledAtLeastOnce();

            final List<String> peerList = objectUnderTest.getPeerList();
            assertThat(peerList, notNullValue());
            assertThat(peerList.size(), equalTo(0));
        }

        @Test
        void getPeerList_returns_list_as_found() {

            final List<String> knownIpPeers = IntStream.range(0, 3)
                    .mapToObj(i -> generateRandomIp())
                    .collect(Collectors.toList());

            final List<HttpInstanceSummary> instances = knownIpPeers
                    .stream()
                    .map(ip -> {
                        final HttpInstanceSummary instanceSummary = mock(HttpInstanceSummary.class);
                        given(instanceSummary.attributes()).willReturn(
                                Collections.singletonMap("AWS_INSTANCE_IPV4", ip));
                        return instanceSummary;
                    })
                    .collect(Collectors.toList());

            given(discoverInstancesResponse.instances()).willReturn(instances);

            final AwsCloudMapPeerListProvider objectUnderTest = createObjectUnderTest();

            waitUntilPeerListPopulated(objectUnderTest);

            final List<String> actualPeers = objectUnderTest.getPeerList();
            assertThat(actualPeers, notNullValue());
            assertThat(actualPeers.size(), equalTo(instances.size()));

            assertThat(new HashSet<>(actualPeers), equalTo(new HashSet<>(knownIpPeers)));
        }

        @Test
        void constructor_continues_to_discover_instances() {

            createObjectUnderTest();

            waitUntilDiscoverInstancesCalledAtLeast(2);

            final ArgumentCaptor<DiscoverInstancesRequest> requestArgumentCaptor =
                    ArgumentCaptor.forClass(DiscoverInstancesRequest.class);

            then(awsServiceDiscovery)
                    .should(atLeast(2))
                    .discoverInstances(requestArgumentCaptor.capture());

            for (DiscoverInstancesRequest actualRequest : requestArgumentCaptor.getAllValues()) {
                assertThat(actualRequest.namespaceName(), equalTo(namespaceName));
                assertThat(actualRequest.serviceName(), equalTo(serviceName));
                assertThat(actualRequest.queryParameters(), equalTo(queryParameters));
                assertThat(actualRequest.healthStatusAsString(), nullValue());
            }
        }
    }

    @Nested
    class WithSeveralFailedAttempts {

        private List<String> knownIpPeers;

        @BeforeEach
        void setUp() {
            final DiscoverInstancesResponse discoverInstancesResponse = mock(DiscoverInstancesResponse.class);

            knownIpPeers = IntStream.range(0, 3)
                    .mapToObj(i -> generateRandomIp())
                    .collect(Collectors.toList());

            final List<HttpInstanceSummary> instances = knownIpPeers
                    .stream()
                    .map(ip -> {
                        final HttpInstanceSummary instanceSummary = mock(HttpInstanceSummary.class);
                        given(instanceSummary.attributes()).willReturn(
                                Collections.singletonMap("AWS_INSTANCE_IPV4", ip));
                        return instanceSummary;
                    })
                    .collect(Collectors.toList());

            given(discoverInstancesResponse.instances()).willReturn(instances);

            final CompletableFuture<DiscoverInstancesResponse> failedFuture1 = new CompletableFuture<>();
            failedFuture1.completeExceptionally(mock(Throwable.class));
            final CompletableFuture<DiscoverInstancesResponse> failedFuture2 = new CompletableFuture<>();
            failedFuture2.completeExceptionally(mock(Throwable.class));
            final CompletableFuture<DiscoverInstancesResponse> successFuture = CompletableFuture.completedFuture(discoverInstancesResponse);

            given(awsServiceDiscovery.discoverInstances(any(DiscoverInstancesRequest.class)))
                    .willReturn(failedFuture1)
                    .willReturn(failedFuture2)
                    .willReturn(successFuture);

            given(backoff.nextDelayMillis(anyInt()))
                    .willReturn(100L);

        }

        @Test
        void getPeerList_returns_value_after_several_failed_attempts() {

            final AwsCloudMapPeerListProvider objectUnderTest = createObjectUnderTest();

            waitUntilDiscoverInstancesCalledAtLeastOnce();

            final List<String> expectedEmpty = objectUnderTest.getPeerList();

            assertThat(expectedEmpty, notNullValue());
            assertThat(expectedEmpty.size(), equalTo(0));

            waitUntilPeerListPopulated(objectUnderTest);

            final List<String> expectedPopulated = objectUnderTest.getPeerList();

            assertThat(expectedPopulated, notNullValue());
            assertThat(expectedPopulated.size(), equalTo(knownIpPeers.size()));

            assertThat(new HashSet<>(expectedPopulated), equalTo(new HashSet<>(knownIpPeers)));

            final InOrder inOrder = inOrder(backoff);
            then(backoff)
                    .should(inOrder)
                    .nextDelayMillis(1);
            then(backoff)
                    .should(inOrder)
                    .nextDelayMillis(2);
            then(backoff)
                    .shouldHaveNoMoreInteractions();
        }

        @Test
        void listener_gets_list_after_several_failed_attempts() {

            final List<Endpoint> listenerEndpoints = new ArrayList<>();

            final AwsCloudMapPeerListProvider objectUnderTest = createObjectUnderTest();

            objectUnderTest.addListener(listenerEndpoints::addAll);

            waitUntilDiscoverInstancesCalledAtLeastOnce();

            assertThat(listenerEndpoints.size(), equalTo(0));

            waitUntilPeerListPopulated(objectUnderTest);

            assertThat(listenerEndpoints.size(), equalTo(knownIpPeers.size()));

            final Set<String> observedIps = listenerEndpoints.stream()
                    .map(Endpoint::ipAddr)
                    .collect(Collectors.toSet());

            assertThat(observedIps, equalTo(new HashSet<>(knownIpPeers)));

            final InOrder inOrder = inOrder(backoff);
            then(backoff)
                    .should(inOrder)
                    .nextDelayMillis(1);
            then(backoff)
                    .should(inOrder)
                    .nextDelayMillis(2);
            then(backoff)
                    .shouldHaveNoMoreInteractions();
        }
    }

    private void waitUntilDiscoverInstancesCalledAtLeastOnce() {
        waitUntilDiscoverInstancesCalledAtLeast(1);
    }

    /**
     * Waits for DiscoverInstances to be called at least a specified number
     * of times. This method intentionally does not inspect the request.
     *
     * @param timesCalled The number of times to wait for it to be called.
     */
    private void waitUntilDiscoverInstancesCalledAtLeast(final int timesCalled) {
        final long waitTimeMillis = (long) timesCalled * WAIT_TIME_MULTIPLIER_MILLIS;
        await().atMost(waitTimeMillis, TimeUnit.MILLISECONDS)
                .pollDelay(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> then(awsServiceDiscovery)
                        .should(atLeast(timesCalled))
                        .discoverInstances(ArgumentMatchers.any(DiscoverInstancesRequest.class)));
    }

    /**
     * Waits until the give {@link AwsCloudMapPeerListProvider} has a peer list
     * with a value greater than 0.
     *
     * @param objectUnderTest The object to wait for.
     */
    private void waitUntilPeerListPopulated(final AwsCloudMapPeerListProvider objectUnderTest) {
        await().atMost(5, TimeUnit.SECONDS)
                .pollDelay(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    final List<String> actualPeers = objectUnderTest.getPeerList();
                    assertThat(actualPeers, notNullValue());
                    assertThat(actualPeers.size(), greaterThan(0));
                });
    }

    private static String generateRandomIp() {
        final Random random = new Random();

        return IntStream.range(0, 4)
                .map(i -> random.nextInt(255))
                .mapToObj(Integer::toString)
                .collect(Collectors.joining("."));
    }

    private static Map<String, String> generateRandomStringMap() {
        final Random random = new Random();

        final Map<String, String> map = new HashMap<>();
        IntStream.range(0, random.nextInt(5) + 1)
                .forEach(num -> map.put(UUID.randomUUID().toString(), UUID.randomUUID().toString()));

        return map;
    }
}
