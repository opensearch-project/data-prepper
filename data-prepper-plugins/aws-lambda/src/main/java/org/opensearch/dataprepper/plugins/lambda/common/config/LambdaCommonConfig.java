package org.opensearch.dataprepper.plugins.lambda.common.config;

import java.time.Duration;

public class LambdaCommonConfig {
    public static final int DEFAULT_CONNECTION_RETRIES = 3;
    public static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(60);

    public static final String STS_REGION = "region";
    public static final String STS_ROLE_ARN = "sts_role_arn";

}
