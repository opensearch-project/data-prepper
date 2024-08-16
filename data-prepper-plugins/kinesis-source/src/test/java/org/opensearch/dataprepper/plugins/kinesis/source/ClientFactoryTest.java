package org.opensearch.dataprepper.plugins.kinesis.source;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.AwsAuthenticationConfig;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

public class ClientFactoryTest {
    private Region region = Region.US_EAST_1;
    private String roleArn;
    private Map<String, String> stsHeader;
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Test
    void testCreateClient() throws NoSuchFieldException, IllegalAccessException {
        roleArn = "arn:aws:iam::278936200144:role/test-role";
        stsHeader= new HashMap<>();
        stsHeader.put(UUID.randomUUID().toString(),UUID.randomUUID().toString());
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);

        AwsAuthenticationConfig awsAuthenticationOptionsConfig = new AwsAuthenticationConfig();
        ReflectivelySetField.setField(AwsAuthenticationConfig.class, awsAuthenticationOptionsConfig, "awsRegion", "us-east-1");
        ReflectivelySetField.setField(AwsAuthenticationConfig.class, awsAuthenticationOptionsConfig, "awsStsRoleArn", roleArn);

        ClientFactory clientFactory = new ClientFactory(awsCredentialsSupplier, awsAuthenticationOptionsConfig);

        final DynamoDbAsyncClient dynamoDbAsyncClient = clientFactory.buildDynamoDBClient();
        assertNotNull(dynamoDbAsyncClient);

        final KinesisAsyncClient kinesisAsyncClient = clientFactory.buildKinesisAsyncClient();
        assertNotNull(kinesisAsyncClient);

        final CloudWatchAsyncClient cloudWatchAsyncClient = clientFactory.buildCloudWatchAsyncClient();
        assertNotNull(cloudWatchAsyncClient);
    }
}
