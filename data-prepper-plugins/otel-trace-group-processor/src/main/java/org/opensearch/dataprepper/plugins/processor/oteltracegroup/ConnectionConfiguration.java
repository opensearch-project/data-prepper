package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import software.amazon.awssdk.arns.Arn;

import java.nio.file.Path;
import java.time.temporal.ValueRange;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ConnectionConfiguration {
    private static final String AWS_IAM_ROLE = "role";
    private static final String AWS_IAM = "iam";
    private static final String DEFAULT_AWS_REGION = "us-east-1";
    static final String AOS_SERVICE_NAME = "es";

    public static final String HOSTS = "hosts";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String SOCKET_TIMEOUT = "socket_timeout";
    public static final String CONNECT_TIMEOUT = "connect_timeout";
    public static final String CERT_PATH = "cert";
    public static final String INSECURE = "insecure";
    public static final String AUTHENTICATION = "authentication";
    public static final String AWS_OPTION = "aws";
    public static final String AWS_SIGV4 = "aws_sigv4";
    public static final String AWS_REGION = "aws_region";
    public static final String AWS_STS_ROLE_ARN = "aws_sts_role_arn";
    public static final String AWS_STS_EXTERNAL_ID = "aws_sts_external_id";
    public static final String AWS_STS_HEADER_OVERRIDES = "aws_sts_header_overrides";
    public static final String PROXY = "proxy";

    /**
     * The valid port range per https://tools.ietf.org/html/rfc6335.
     */
    static final ValueRange VALID_PORT_RANGE = ValueRange.of(0, 65535);

    @JsonProperty(HOSTS)
    @NotEmpty
    private List<String> hosts;

    @JsonProperty(USERNAME)
    private String username;

    @JsonProperty(PASSWORD)
    private String password;

    @JsonProperty(CERT_PATH)
    private Path certPath;

    @JsonProperty(SOCKET_TIMEOUT)
    private Integer socketTimeout;

    @JsonProperty(CONNECT_TIMEOUT)
    private Integer connectTimeout;

    @JsonProperty(INSECURE)
    private boolean insecure;

    @JsonProperty(AWS_SIGV4)
    private boolean awsSigv4;

    @JsonProperty(AWS_REGION)
    private String awsRegion = DEFAULT_AWS_REGION;

    @JsonProperty(AWS_STS_ROLE_ARN)
    @Size(max = 2048, message = "aws_sts_role_arn length cannot exceed 2048")
    private String awsStsRoleArn;

    @JsonProperty(AWS_OPTION)
    private AwsOption awsOption;

    @JsonProperty(AWS_STS_EXTERNAL_ID)
    @Size(max = 1224, message = "aws_sts_external_id length cannot exceed 1224")
    private String awsStsExternalId;

    @JsonProperty(AWS_STS_HEADER_OVERRIDES)
    private Map<String, String> awsStsHeaderOverrides;

    @JsonProperty(PROXY)
    private String proxy;

    @JsonProperty(AUTHENTICATION)
    private AuthConfig authConfig;

    List<String> getHosts() {
        return hosts;
    }

    String getUsername() {
        return username;
    }

    String getPassword() {
        return password;
    }

    boolean isAwsSigv4() {
        return awsSigv4 || awsOption != null;
    }

    String getAwsRegion() {
        return awsRegion;
    }

    String getAwsStsRoleArn() {
        return awsStsRoleArn;
    }

    AwsOption getAwsOption() {
        return awsOption;
    }

    Path getCertPath() {
        return certPath;
    }

    String getProxy() {
        return proxy;
    }

    Integer getSocketTimeout() {
        return socketTimeout;
    }

    Integer getConnectTimeout() {
        return connectTimeout;
    }

    public AuthConfig getAuthConfig() {
        return authConfig;
    }

    public String getAwsStsExternalId() {
        return awsStsExternalId;
    }

    public Map<String, String> getAwsStsHeaderOverrides() {
        return awsStsHeaderOverrides;
    }

    public boolean isInsecure() {
        return insecure;
    }

    @AssertTrue(message = "Deprecated username and password should not be set " +
            "when authentication is configured.")
    boolean isValidAuthentication() {
        if (authConfig != null) {
            return username == null && password == null;
        }
        if (username != null || password != null) {
            return authConfig == null;
        }
        return true;
    }

    @AssertTrue(message = "aws_sigv4 option cannot be used along with aws option.")
    boolean isValidAwsAuth() {
        // TODO: fill-in custom logic
        if (awsOption != null) {
            return !awsSigv4;
        }
        if (awsSigv4) {
            return awsOption == null;
        }
        return true;
    }

    @AssertTrue(message = "sts_role_arn must be an null or a valid IAM role ARN.")
    boolean isValidStsRoleArn() {
        if (awsStsRoleArn == null) {
            return true;
        }
        final Arn arn = getArn(awsStsRoleArn);
        if (!AWS_IAM.equals(arn.service())) {
            return false;
        }
        final Optional<String> resourceType = arn.resource().resourceType();
        if (resourceType.isEmpty() || !resourceType.get().equals(AWS_IAM_ROLE)) {
            return false;
        }
        return true;
    }

    private Arn getArn(final String awsStsRoleArn) {
        try {
            return Arn.fromString(awsStsRoleArn);
        } catch (final Exception e) {
            throw new IllegalArgumentException(String.format("Invalid ARN format for awsStsRoleArn. Check the format of %s", awsStsRoleArn));
        }
    }
}
