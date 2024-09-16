package org.opensearch.dataprepper.plugins.lambda.common.config;

public class LambdaCommonConfig {
    public static final String REQUEST_RESPONSE = "request-response";
    public static final String EVENT = "event";
    public static final String BATCH_EVENT = "batch-event";
    public static final String SINGLE_EVENT = "single-event";

    //AWS Lambda payload options needs this format
    public static final String REQUEST_RESPONSE_LAMBDA = "RequestResponse";
    public static final String EVENT_LAMBDA = "Event";
}
