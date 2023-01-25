package org.opensearch.dataprepper.plugins.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.s3.S3Client;

public class S3SinkService {
	
	private static final Logger LOG = LoggerFactory.getLogger(S3SinkService.class);
	
	private final S3SinkConfig s3SinkConfig;
	private final S3Client s3Client;
	
	S3SinkService(final S3SinkConfig s3SinkConfig){
		this.s3SinkConfig = s3SinkConfig;
		this.s3Client = createS3Client();
	}

	S3Client createS3Client() {
        LOG.info("Creating S3 client");
        return S3Client.builder()
                .region(s3SinkConfig.getSinkAwsAuthenticationOptions().getAwsRegion())
                .credentialsProvider(s3SinkConfig.getSinkAwsAuthenticationOptions().authenticateAwsConfiguration())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder().numRetries(5).build())
                        .build())
                .build();
    }

}
