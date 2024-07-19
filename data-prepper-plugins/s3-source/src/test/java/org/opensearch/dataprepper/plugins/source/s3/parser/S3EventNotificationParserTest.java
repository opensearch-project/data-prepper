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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3EventNotificationParserTest {
    static final String DIRECT_SQS_MESSAGE =
            "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\",\"eventTime\":\"2023-04-28T16:00:11.324Z\"," +
                    "\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:xyz\"},\"requestParameters\":{\"sourceIPAddress\":\"127.0.0.1\"}," +
                    "\"responseElements\":{\"x-amz-request-id\":\"xyz\",\"x-amz-id-2\":\"xyz\"},\"s3\":{\"s3SchemaVersion\":\"1.0\"," +
                    "\"configurationId\":\"xyz\",\"bucket\":{\"name\":\"my-bucket\",\"ownerIdentity\":{\"principalId\":\"ABC\"}," +
                    "\"arn\":\"arn:aws:s3:::my-bucket\"},\"object\":{\"key\":\"path/to/myfile.log.gz\",\"size\":3159112,\"eTag\":\"abcd123\"," +
                    "\"sequencer\":\"000\"}}}]}";

    public static final String SNS_BASED_MESSAGE = "{\n" +
            "  \"Type\" : \"Notification\",\n" +
            "  \"MessageId\" : \"4e01e115-5b91-5096-8a74-bee95ed1e123\",\n" +
            "  \"TopicArn\" : \"arn:aws:sns:us-east-1:123456789012:notifications\",\n" +
            "  \"Subject\" : \"Amazon S3 Notification\",\n" +
            "  \"Message\" : \"{\\\"Records\\\":[{\\\"eventVersion\\\":\\\"2.1\\\",\\\"eventSource\\\":\\\"aws:s3\\\",\\\"awsRegion\\\":\\\"us-east-1\\\",\\\"eventTime\\\":\\\"2023-05-02T13:37:03.502Z\\\",\\\"eventName\\\":\\\"ObjectCreated:Put\\\",\\\"userIdentity\\\":{\\\"principalId\\\":\\\"AWS:ABC\\\"},\\\"requestParameters\\\":{\\\"sourceIPAddress\\\":\\\"127.0.0.1\\\"},\\\"responseElements\\\":{\\\"x-amz-request-id\\\":\\\"ABC\\\",\\\"x-amz-id-2\\\":\\\"ABC\\\"},\\\"s3\\\":{\\\"s3SchemaVersion\\\":\\\"1.0\\\",\\\"configurationId\\\":\\\"S3ToSnsTest\\\",\\\"bucket\\\":{\\\"name\\\":\\\"my-sns-bucket\\\",\\\"ownerIdentity\\\":{\\\"principalId\\\":\\\"ABC\\\"},\\\"arn\\\":\\\"arn:aws:s3:::my-sns-bucket\\\"},\\\"object\\\":{\\\"key\\\":\\\"path/to/testlogs.log.gz\\\",\\\"size\\\":25,\\\"eTag\\\":\\\"abc\\\",\\\"sequencer\\\":\\\"ABC\\\"}}}]}\",\n" +
            "  \"Timestamp\" : \"2023-05-02T13:37:04.554Z\",\n" +
            "  \"SignatureVersion\" : \"1\",\n" +
            "  \"Signature\" : \"x//abcde==\",\n" +
            "  \"SigningCertURL\" : \"https://sns.us-east-1.amazonaws.com/SimpleNotificationService.pem\",\n" +
            "  \"UnsubscribeURL\" : \"https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:123456789012:notifications:abc\"\n" +
            "}";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private Message message;
    @BeforeEach
    public void setUp() {
        message = mock(Message.class);
    }

    private S3EventNotificationParser createObjectUnderTest() {
        return new S3EventNotificationParser();
    }

    @Test
    void parseMessage_returns_expected_ParsedMessage_from_SQS_message() {
        when(message.body()).thenReturn(DIRECT_SQS_MESSAGE);
        final ParsedMessage parsedMessage = createObjectUnderTest().parseMessage(message, objectMapper);

        assertThat(parsedMessage, notNullValue());
        assertThat(parsedMessage.getMessage(), notNullValue());

        assertThat(parsedMessage.getBucketName(), equalTo("my-bucket"));
        assertThat(parsedMessage.getEventName(), equalTo("ObjectCreated:Put"));
        assertThat(parsedMessage.getObjectKey(), equalTo("path/to/myfile.log.gz"));
    }

    @Test
    void parseMessage_returns_expected_ParsedMessage_from_SNS_to_SQS_message() {
        when(message.body()).thenReturn(SNS_BASED_MESSAGE);
        final ParsedMessage parsedMessage = createObjectUnderTest().parseMessage(message, objectMapper);

        assertThat(parsedMessage, notNullValue());
        assertThat(parsedMessage.getMessage(), notNullValue());
        assertThat(parsedMessage.getBucketName(), equalTo("my-sns-bucket"));
        assertThat(parsedMessage.getObjectKey(), equalTo("path/to/testlogs.log.gz"));
        assertThat(parsedMessage.getEventName(), equalTo("ObjectCreated:Put"));
    }

    @Test
    void parseMessage_fails_for_TestEvent() {
        final String testEventMessage = "{\"Service\":\"Amazon S3\",\"Event\":\"s3:TestEvent\",\"Time\":\"2022-10-15T16:36:25.510Z\"," +
                "\"Bucket\":\"bucketname\",\"RequestId\":\"abcdefg\",\"HostId\":\"hijklm\"}";

        when(message.body()).thenReturn(testEventMessage);

        final ParsedMessage parsedMessage = createObjectUnderTest().parseMessage(message, objectMapper);

        assertThat(parsedMessage, notNullValue());
        assertThat(parsedMessage.isFailedParsing(), equalTo(false));
        assertThat(parsedMessage.isEmptyNotification(), equalTo(true));
    }
}
