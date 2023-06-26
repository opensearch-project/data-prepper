package org.opensearch.dataprepper.plugins.aws.sqs.common;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class ClientFactoryTest {

    private Region region = Region.US_EAST_1;
    private String roleArn;
    private Map<String, String> stsHeader;
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Test
    void create_sqs_client_test(){
        roleArn = "arn:aws:iam::278936200144:role/test-role";
        stsHeader= new HashMap<>();
        stsHeader.put(UUID.randomUUID().toString(),UUID.randomUUID().toString());
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        final SqsClient sqsClient = ClientFactory.createSqsClient(region, roleArn, stsHeader, awsCredentialsSupplier);
        assertThat(sqsClient,notNullValue());
    }
}
