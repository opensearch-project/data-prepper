/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

class S3EventMessageParserTest {
    private static final String DIRECT_SQS_MESSAGE =
            "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\",\"eventTime\":\"2023-04-28T16:00:11.324Z\"," +
                    "\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:xyz\"},\"requestParameters\":{\"sourceIPAddress\":\"127.0.0.1\"}," +
                    "\"responseElements\":{\"x-amz-request-id\":\"xyz\",\"x-amz-id-2\":\"xyz\"},\"s3\":{\"s3SchemaVersion\":\"1.0\"," +
                    "\"configurationId\":\"xyz\",\"bucket\":{\"name\":\"my-bucket\",\"ownerIdentity\":{\"principalId\":\"ABC\"}," +
                    "\"arn\":\"arn:aws:s3:::my-bucket\"},\"object\":{\"key\":\"path/to/myfile.log.gz\",\"size\":3159112,\"eTag\":\"abcd123\"," +
                    "\"sequencer\":\"000\"}}}]}";

    private static final String SNS_BASED_MESSAGE = "{\n" +
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

    private S3EventMessageParser createObjectUnderTest() {
        return new S3EventMessageParser();
    }

    @Test
    void parseMessage_returns_expected_S3EventNotification_from_SQS_message() throws JsonProcessingException {
        final S3EventNotification s3EventNotification = createObjectUnderTest().parseMessage(DIRECT_SQS_MESSAGE);

        assertThat(s3EventNotification, notNullValue());
        assertThat(s3EventNotification.getRecords(), notNullValue());
        assertThat(s3EventNotification.getRecords(), hasSize(1));
        final S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord = s3EventNotification.getRecords().get(0);
        assertThat(s3EventNotificationRecord, notNullValue());
        assertThat(s3EventNotificationRecord.getEventName(), equalTo("ObjectCreated:Put"));
        assertThat(s3EventNotificationRecord.getS3(), notNullValue());
        assertThat(s3EventNotificationRecord.getS3().getBucket(), notNullValue());
        assertThat(s3EventNotificationRecord.getS3().getBucket().getName(), equalTo("my-bucket"));
        assertThat(s3EventNotificationRecord.getS3().getObject(), notNullValue());
        assertThat(s3EventNotificationRecord.getS3().getObject().getKey(), equalTo("path/to/myfile.log.gz"));
    }

    @Test
    void parseMessage_returns_expected_S3EventNotification_from_SNS_to_SQS_message() throws JsonProcessingException {
        final S3EventNotification s3EventNotification = createObjectUnderTest().parseMessage(SNS_BASED_MESSAGE);

        assertThat(s3EventNotification, notNullValue());
        assertThat(s3EventNotification.getRecords(), notNullValue());
        assertThat(s3EventNotification.getRecords(), hasSize(1));
        final S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord = s3EventNotification.getRecords().get(0);
        assertThat(s3EventNotificationRecord, notNullValue());
        assertThat(s3EventNotificationRecord.getEventName(), equalTo("ObjectCreated:Put"));
        assertThat(s3EventNotificationRecord.getS3(), notNullValue());
        assertThat(s3EventNotificationRecord.getS3().getBucket(), notNullValue());
        assertThat(s3EventNotificationRecord.getS3().getBucket().getName(), equalTo("my-sns-bucket"));
        assertThat(s3EventNotificationRecord.getS3().getObject(), notNullValue());
        assertThat(s3EventNotificationRecord.getS3().getObject().getKey(), equalTo("path/to/testlogs.log.gz"));
    }

    @Test
    void parseMessage_throws_for_TestEvent() {
        final String testEventMessage = "{\"Service\":\"Amazon S3\",\"Event\":\"s3:TestEvent\",\"Time\":\"2022-10-15T16:36:25.510Z\"," +
                "\"Bucket\":\"bucketname\",\"RequestId\":\"abcdefg\",\"HostId\":\"hijklm\"}";

        final S3EventMessageParser objectUnderTest = createObjectUnderTest();

        assertThrows(JsonProcessingException.class, () -> objectUnderTest.parseMessage(testEventMessage));
    }
}