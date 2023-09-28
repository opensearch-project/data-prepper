/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class S3EventBridgeNotificationParserTest {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String EVENTBRIDGE_MESSAGE = "{\"version\":\"0\",\"id\":\"17793124-05d4-b198-2fde-7ededc63b103\",\"detail-type\":\"Object Created\"," +
            "\"source\":\"aws.s3\",\"account\":\"111122223333\",\"time\":\"2021-11-12T00:00:00Z\"," +
            "\"region\":\"ca-central-1\",\"resources\":[\"arn:aws:s3:::DOC-EXAMPLE-BUCKET1\"]," +
            "\"detail\":{\"version\":\"0\",\"bucket\":{\"name\":\"DOC-EXAMPLE-BUCKET1\"}," +
            "\"object\":{\"key\":\"example-key\",\"size\":5,\"etag\":\"b1946ac92492d2347c6235b4d2611184\"," +
            "\"version-id\":\"IYV3p45BT0ac8hjHg1houSdS1a.Mro8e\",\"sequencer\":\"617f08299329d189\"}," +
            "\"request-id\":\"N4N7GDK58NMKJ12R\",\"requester\":\"123456789012\",\"source-ip-address\":\"1.2.3.4\"," +
            "\"reason\":\"PutObject\"}}";

    private final String SECURITY_LAKE_MESSAGE = "{\"source\":\"aws.s3\",\"time\":\"2021-11-12T00:00:00Z\",\"account\":\"123456789012\",\"region\":\"ca-central-1\"," +
            "\"resources\":[\"arn:aws:s3:::example-bucket\"],\"detail\":{\"bucket\":{\"name\":\"example-bucket\"}," +
            "\"object\":{\"key\":\"example-key\",\"size\":5," +
            "\"etag\":\"b57f9512698f4b09e608f4f2a65852e5\"},\"request-id\":\"N4N7GDK58NMKJ12R\"," +
            "\"requester\":\"securitylake.amazonaws.com\"}}";
    private Message message;
    @BeforeEach
    public void setUp() {
        message = mock(Message.class);
    }

    private S3EventBridgeNotificationParser createObjectUnderTest() {
        return new S3EventBridgeNotificationParser();
    }

    @Test
    void parseMessage_returns_expected_ParsedMessage_from_eventbridge_message() {
        when(message.body()).thenReturn(EVENTBRIDGE_MESSAGE);
        final ParsedMessage parsedMessage = createObjectUnderTest().parseMessage(message, objectMapper);

        assertThat(parsedMessage, notNullValue());
        assertThat(parsedMessage.getMessage(), notNullValue());

        assertThat(parsedMessage.getBucketName(), equalTo("DOC-EXAMPLE-BUCKET1"));
        assertThat(parsedMessage.getObjectKey(), equalTo("example-key"));
        assertThat(parsedMessage.getDetailType(), equalTo("Object Created"));
    }

    @Test
    void parseMessage_returns_expected_ParsedMessage_from_security_lake_message() {
        when(message.body()).thenReturn(SECURITY_LAKE_MESSAGE);
        final ParsedMessage parsedMessage = createObjectUnderTest().parseMessage(message, objectMapper);

        assertThat(parsedMessage, notNullValue());
        assertThat(parsedMessage.getMessage(), notNullValue());
        assertThat(parsedMessage.getBucketName(), equalTo("example-bucket"));
        assertThat(parsedMessage.getObjectKey(), equalTo("example-key"));
        assertThat(parsedMessage.getEventName(), nullValue());
    }
}
