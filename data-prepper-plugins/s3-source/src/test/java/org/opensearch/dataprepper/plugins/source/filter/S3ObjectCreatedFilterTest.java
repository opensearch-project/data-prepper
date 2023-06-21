/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.filter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.opensearch.dataprepper.plugins.source.parser.ParsedMessage;

class S3ObjectCreatedFilterTest {

    private S3ObjectCreatedFilter s3ObjectCreatedFilter;
    private ParsedMessage parsedMessage;


    @BeforeEach
    void setUp() {
        s3ObjectCreatedFilter = new S3ObjectCreatedFilter();
        parsedMessage = mock(ParsedMessage.class);
    }

    @Test
    void filter_with_eventName_ObjectCreated_should_return_non_empty_instance_of_optional() {
        when(parsedMessage.getEventName()).thenReturn("ObjectCreated:Put");
        Optional<ParsedMessage> actualValue = s3ObjectCreatedFilter.filter(parsedMessage);

        assertThat(actualValue, instanceOf(Optional.class));
        assertTrue(actualValue.isPresent());
        assertThat(actualValue, equalTo(Optional.of(parsedMessage)));
    }

    @Test
    void filter_with_eventName_ObjectRemoved_should_return_empty_instance_of_optional() {
        when(parsedMessage.getEventName()).thenReturn("ObjectRemoved:Delete");
        Optional<ParsedMessage> actualValue = s3ObjectCreatedFilter.filter(parsedMessage);

        assertThat(actualValue, instanceOf(Optional.class));
        assertFalse(actualValue.isPresent());
        assertThat(actualValue, equalTo(Optional.empty()));
    }
}
