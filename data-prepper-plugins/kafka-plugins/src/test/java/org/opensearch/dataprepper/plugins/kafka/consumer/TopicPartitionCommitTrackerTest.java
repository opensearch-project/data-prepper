/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.commons.lang3.Range;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@ExtendWith(MockitoExtension.class)
class TopicPartitionCommitTrackerTest {
    private final String testTopic = "test_topic";
    private final int testPartition = 1;
    private TopicPartitionCommitTracker topicPartitionCommitTracker;
    public TopicPartitionCommitTracker createObjectUnderTest(String topic, int partition, Long offset) {
        return new TopicPartitionCommitTracker(topic, partition, offset);
    }

    @ParameterizedTest
    @MethodSource("getInputOrder")
    public void test(List<Integer> order) {
        topicPartitionCommitTracker = createObjectUnderTest(testTopic, testPartition, 0L);
        List<Range<Long>> ranges = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ranges.add(Range.between(i*10L, i*10L+9L));
        }
        OffsetAndMetadata result = null;
        Long expectedOffset = 10L;
        for (Integer i: order) {
            result = topicPartitionCommitTracker.addCompletedOffsets(ranges.get(i));
            if (ranges.get(i).getMaximum() == (expectedOffset - 1)) {
                assertThat(result.offset(), greaterThanOrEqualTo(expectedOffset));
                expectedOffset = result.offset() + 10L;
            }
        }
        assertTrue(Objects.nonNull(result));
        assertThat(result.offset(), equalTo(100L));
    }

    private static Stream<Arguments> getInputOrder() {
        List<List<Integer>> orderList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            List<Integer> order = new ArrayList<>();
            for (int j = 0; j < 10; j++) {
                order.add(j);
            }
            Collections.shuffle(order);
            orderList.add(order);
        }
        return Stream.of(
            Arguments.of(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)),
            Arguments.of(List.of(9, 8, 7, 6, 5, 4, 3, 2, 1, 0)),
            Arguments.of(orderList.get(0)),
            Arguments.of(orderList.get(1)),
            Arguments.of(orderList.get(2)),
            Arguments.of(orderList.get(3)),
            Arguments.of(orderList.get(4)),
            Arguments.of(orderList.get(5)),
            Arguments.of(orderList.get(6)),
            Arguments.of(orderList.get(7)),
            Arguments.of(orderList.get(8)),
            Arguments.of(orderList.get(9))
        );
    }

}
