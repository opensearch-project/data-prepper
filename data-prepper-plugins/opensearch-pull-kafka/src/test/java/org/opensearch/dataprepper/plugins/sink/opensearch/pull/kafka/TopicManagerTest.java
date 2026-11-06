/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.pull.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.ReassignmentInProgressException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TopicManagerTest {

    @Mock
    private KafkaPullEngineConfig config;

    private String topicName;
    private int partitionCount;

    @BeforeEach
    void setUp() {
        topicName = UUID.randomUUID().toString();
        partitionCount = 5;

        when(config.getBootstrapServers()).thenReturn(List.of("localhost:9092"));
    }

    private TopicManager createObjectUnderTest() {
        return new TopicManager(config);
    }

    @Test
    void createTopicWithPartitions_creates_new_topic() throws Exception {
        final AdminClient mockAdmin = mock(AdminClient.class);
        mockCreateTopicsSuccess(mockAdmin);
        mockDescribeTopics(mockAdmin, partitionCount);

        try (final MockedStatic<AdminClient> adminStatic = mockStatic(AdminClient.class)) {
            adminStatic.when(() -> AdminClient.create(any(java.util.Properties.class))).thenReturn(mockAdmin);

            createObjectUnderTest().createTopicWithPartitions(topicName, partitionCount);
        }

        verify(mockAdmin).createTopics(any());
        verify(mockAdmin, never()).createPartitions(any());
    }

    @Test
    void createTopicWithPartitions_expands_partitions_when_topic_exists_with_fewer() throws Exception {
        final AdminClient mockAdminCreate = mock(AdminClient.class);
        mockCreateTopicsFailsWithExists(mockAdminCreate);

        final AdminClient mockAdminEnsure = mock(AdminClient.class);
        mockDescribeTopicsSequential(mockAdminEnsure, 1, partitionCount);
        mockCreatePartitionsSuccess(mockAdminEnsure);

        try (final MockedStatic<AdminClient> adminStatic = mockStatic(AdminClient.class)) {
            adminStatic.when(() -> AdminClient.create(any(java.util.Properties.class)))
                    .thenReturn(mockAdminCreate, mockAdminEnsure);

            createObjectUnderTest().createTopicWithPartitions(topicName, partitionCount);
        }

        verify(mockAdminEnsure).createPartitions(any());
    }

    @Test
    void createTopicWithPartitions_does_not_expand_when_topic_has_enough_partitions() throws Exception {
        final AdminClient mockAdminCreate = mock(AdminClient.class);
        mockCreateTopicsFailsWithExists(mockAdminCreate);

        final AdminClient mockAdminEnsure = mock(AdminClient.class);
        mockDescribeTopics(mockAdminEnsure, partitionCount);

        try (final MockedStatic<AdminClient> adminStatic = mockStatic(AdminClient.class)) {
            adminStatic.when(() -> AdminClient.create(any(java.util.Properties.class)))
                    .thenReturn(mockAdminCreate, mockAdminEnsure);

            createObjectUnderTest().createTopicWithPartitions(topicName, partitionCount);
        }

        verify(mockAdminEnsure, never()).createPartitions(any());
    }

    @Test
    void createTopicWithPartitions_throws_on_non_exists_execution_exception() throws Exception {
        final AdminClient mockAdmin = mock(AdminClient.class);
        final CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
        final KafkaFuture<Void> future = mock(KafkaFuture.class);
        when(createTopicsResult.all()).thenReturn(future);
        when(future.get()).thenThrow(new ExecutionException(new RuntimeException("broker error")));
        when(mockAdmin.createTopics(any())).thenReturn(createTopicsResult);

        try (final MockedStatic<AdminClient> adminStatic = mockStatic(AdminClient.class)) {
            adminStatic.when(() -> AdminClient.create(any(java.util.Properties.class))).thenReturn(mockAdmin);

            final RuntimeException thrown = assertThrows(RuntimeException.class,
                    () -> createObjectUnderTest().createTopicWithPartitions(topicName, partitionCount));
            assertThat(thrown.getCause(), instanceOf(ExecutionException.class));
        }
    }

    @Test
    void createTopicWithPartitions_retries_on_reassignment_in_progress() throws Exception {
        final AdminClient mockAdminCreate = mock(AdminClient.class);
        mockCreateTopicsFailsWithExists(mockAdminCreate);

        final AdminClient mockAdminEnsure = mock(AdminClient.class);
        mockDescribeTopicsSequential(mockAdminEnsure, 1, partitionCount);
        mockCreatePartitionsFailsThenSucceeds(mockAdminEnsure);

        try (final MockedStatic<AdminClient> adminStatic = mockStatic(AdminClient.class)) {
            adminStatic.when(() -> AdminClient.create(any(java.util.Properties.class)))
                    .thenReturn(mockAdminCreate, mockAdminEnsure);

            createObjectUnderTest().createTopicWithPartitions(topicName, partitionCount);
        }

        verify(mockAdminEnsure, times(2)).createPartitions(any());
    }

    @SuppressWarnings("unchecked")
    private void mockCreatePartitionsFailsThenSucceeds(final AdminClient mockAdmin) throws Exception {
        final CreatePartitionsResult failResult = mock(CreatePartitionsResult.class);
        final KafkaFuture<Void> failFuture = mock(KafkaFuture.class);
        when(failResult.all()).thenReturn(failFuture);
        when(failFuture.get()).thenThrow(new ExecutionException(new ReassignmentInProgressException("in progress")));

        final CreatePartitionsResult successResult = mock(CreatePartitionsResult.class);
        final KafkaFuture<Void> successFuture = mock(KafkaFuture.class);
        when(successResult.all()).thenReturn(successFuture);
        when(successFuture.get()).thenReturn(null);

        when(mockAdmin.createPartitions(any())).thenReturn(failResult, successResult);
    }

    @SuppressWarnings("unchecked")
    private void mockCreateTopicsSuccess(final AdminClient mockAdmin) throws Exception {
        final CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
        final KafkaFuture<Void> future = mock(KafkaFuture.class);
        when(createTopicsResult.all()).thenReturn(future);
        when(future.get()).thenReturn(null);
        when(mockAdmin.createTopics(any())).thenReturn(createTopicsResult);
    }

    @SuppressWarnings("unchecked")
    private void mockCreateTopicsFailsWithExists(final AdminClient mockAdmin) throws Exception {
        final CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
        final KafkaFuture<Void> future = mock(KafkaFuture.class);
        when(createTopicsResult.all()).thenReturn(future);
        when(future.get()).thenThrow(new ExecutionException(new TopicExistsException("exists")));
        when(mockAdmin.createTopics(any())).thenReturn(createTopicsResult);
    }

    @SuppressWarnings("unchecked")
    private void mockDescribeTopics(final AdminClient mockAdmin, final int partitionCount) throws Exception {
        final DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        final KafkaFuture<Map<String, TopicDescription>> describeFuture = mock(KafkaFuture.class);
        final TopicDescription description = mock(TopicDescription.class);
        when(description.partitions()).thenReturn(Collections.nCopies(partitionCount, mock(TopicPartitionInfo.class)));
        when(describeFuture.get(any(Long.class), any())).thenReturn(Map.of(topicName, description));
        when(describeResult.allTopicNames()).thenReturn(describeFuture);
        when(mockAdmin.describeTopics(any(java.util.Collection.class))).thenReturn(describeResult);
    }

    @SuppressWarnings("unchecked")
    private void mockDescribeTopicsSequential(final AdminClient mockAdmin, final int firstCount, final int secondCount) throws Exception {
        final DescribeTopicsResult firstResult = mock(DescribeTopicsResult.class);
        final KafkaFuture<Map<String, TopicDescription>> firstFuture = mock(KafkaFuture.class);
        final TopicDescription firstDescription = mock(TopicDescription.class);
        when(firstDescription.partitions()).thenReturn(Collections.nCopies(firstCount, mock(TopicPartitionInfo.class)));
        when(firstFuture.get(any(Long.class), any())).thenReturn(Map.of(topicName, firstDescription));
        when(firstResult.allTopicNames()).thenReturn(firstFuture);

        final DescribeTopicsResult secondResult = mock(DescribeTopicsResult.class);
        final KafkaFuture<Map<String, TopicDescription>> secondFuture = mock(KafkaFuture.class);
        final TopicDescription secondDescription = mock(TopicDescription.class);
        when(secondDescription.partitions()).thenReturn(Collections.nCopies(secondCount, mock(TopicPartitionInfo.class)));
        when(secondFuture.get(any(Long.class), any())).thenReturn(Map.of(topicName, secondDescription));
        when(secondResult.allTopicNames()).thenReturn(secondFuture);

        when(mockAdmin.describeTopics(any(java.util.Collection.class))).thenReturn(firstResult, secondResult);
    }

    @SuppressWarnings("unchecked")
    private void mockCreatePartitionsSuccess(final AdminClient mockAdmin) throws Exception {
        final CreatePartitionsResult createPartitionsResult = mock(CreatePartitionsResult.class);
        final KafkaFuture<Void> future = mock(KafkaFuture.class);
        when(createPartitionsResult.all()).thenReturn(future);
        when(future.get()).thenReturn(null);
        when(mockAdmin.createPartitions(any())).thenReturn(createPartitionsResult);
    }
}
