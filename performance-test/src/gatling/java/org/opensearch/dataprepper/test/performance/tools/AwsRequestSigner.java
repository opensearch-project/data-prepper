package org.opensearch.dataprepper.test.performance.tools;

import io.gatling.http.client.Request;
import io.netty.handler.codec.http.HttpHeaders;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.AwsSignerExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;

public class AwsRequestSigner implements Consumer<Request> {
    static final String SIGNER_NAME = "aws_sigv4";

    /**
     * Constant to check content-length
     */
    private static final String CONTENT_LENGTH = "content-length";
    /**
     * Constant to check Zero content length
     */
    private static final String ZERO_CONTENT_LENGTH = "0";
    /**
     * Constant to check if host is the endpoint
     */
    private static final String HOST = "host";

    private static final String REGION_PROPERTY = "aws_region";
    private static final String SERVICE_NAME_PROPERTY = "aws_service";


    private final Aws4Signer awsSigner;
    private final AwsCredentialsProvider credentialsProvider;
    private final Region region;
    private final String service;

    public AwsRequestSigner() {
        region = getRequiredProperty(REGION_PROPERTY, Region::of);
        service = getRequiredProperty(SERVICE_NAME_PROPERTY, Function.identity());

        awsSigner = Aws4Signer.create();
        credentialsProvider = DefaultCredentialsProvider.create();
    }

    private static <T> T getRequiredProperty(String propertyName, Function<String, T> transform) {
        String inputString = System.getProperty(propertyName);
        if(inputString == null) {
            throw new RuntimeException("Using " + SIGNER_NAME + " authentication requires providing the " + propertyName + " system property.");
        }

        try {
            return transform.apply(inputString);
        } catch (Exception ex) {
            throw new RuntimeException("Unable to process property " + propertyName + " with error: " + ex.getMessage());
        }
    }

    @Override
    public void accept(Request request) {
        ExecutionAttributes attributes = new ExecutionAttributes();
        attributes.putAttribute(AwsSignerExecutionAttribute.AWS_CREDENTIALS, credentialsProvider.resolveCredentials());
        attributes.putAttribute(AwsSignerExecutionAttribute.SERVICE_SIGNING_NAME, service);
        attributes.putAttribute(AwsSignerExecutionAttribute.SIGNING_REGION, region);

        SdkHttpFullRequest incomingSdkRequest = convertIncomingRequest(request);

        SdkHttpFullRequest signedRequest = awsSigner.sign(incomingSdkRequest, attributes);

        modifyOutgoingRequest(request, signedRequest);
    }

    private SdkHttpFullRequest convertIncomingRequest(Request request) {
        URI uri;
        try {
            uri = request.getUri().toJavaNetURI();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        SdkHttpFullRequest.Builder requestBuilder = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.fromValue(request.getMethod().name()))
                .uri(uri);

        requestBuilder.contentStreamProvider(() -> new ByteArrayInputStream(request.getBody().getBytes()));
        requestBuilder.headers(headerArrayToMap(request.getHeaders()));

        return requestBuilder.build();
    }

    private static Map<String, List<String>> headerArrayToMap(final HttpHeaders headers) {
        Map<String, List<String>> headersMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, String> header : headers) {
            if (!skipHeader(header)) {
                headersMap.put(header.getKey(), headersMap
                        .getOrDefault(header.getKey(),
                                new LinkedList<>(Collections.singletonList(header.getValue()))));
            }
        }
        return headersMap;
    }

    private static boolean skipHeader(final Map.Entry<String, String> header) {
        return (CONTENT_LENGTH.equalsIgnoreCase(header.getKey())
                && ZERO_CONTENT_LENGTH.equals(header.getValue())) // Strip Content-Length: 0
                || HOST.equalsIgnoreCase(header.getKey()); // Host comes from endpoint
    }

    private void modifyOutgoingRequest(Request request, SdkHttpFullRequest signedRequest) {
        resetHeaders(request, signedRequest);
    }

    private void resetHeaders(Request request, SdkHttpFullRequest signedRequest) {
        request.getHeaders().clear();

        for (Map.Entry<String, List<String>> headerEntry : signedRequest.headers().entrySet()) {
            for (String value : headerEntry.getValue()) {
                request.getHeaders().add(headerEntry.getKey(), value);
            }
        }
    }
}
