package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import software.amazon.awssdk.arns.Arn;

import java.nio.file.Path;
import java.time.temporal.ValueRange;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@JsonPropertyOrder
@JsonClassDescription("The <code>otel_trace_group</code> processor completes missing trace-group-related fields in the " +
        "collection of <a href=\"https://github.com/opensearch-project/data-prepper/blob/834f28fdf1df6d42a6666e91e6407474b88e7ec6/data-prepper-api/src/main/java/org/opensearch/dataprepper/model/trace/Span.java\">span</a> " +
        "records by looking up the OpenSearch backend. The otel_trace_group processor identifies the missing trace group information for a spanId by looking up the relevant fields in its root <code>span</code> stored in OpenSearch.")
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

    @NotEmpty
    @JsonProperty(HOSTS)
    @JsonPropertyDescription("A list of IP addresses of OpenSearch nodes.")
    private List<String> hosts;

    @JsonProperty(USERNAME)
    @JsonPropertyDescription("A string that contains the username and is used in the " +
            "<a href=\"https://opensearch.org/docs/latest/security/access-control/users-roles/\">internal users</a> YAML configuration file of your OpenSearch cluster.")
    private String username;

    @JsonProperty(PASSWORD)
    @JsonPropertyDescription("A string that contains the password and is used in the " +
            "<a href=\"https://opensearch.org/docs/latest/security/access-control/users-roles/\">internal users</a> YAML configuration file of your OpenSearch cluster.")
    private String password;

    @JsonProperty(CERT_PATH)
    @JsonPropertyDescription("A certificate authority (CA) certificate that is PEM encoded. Accepts both .pem or .crt. " +
            "This enables the client to trust the CA that has signed the certificate that OpenSearch is using.")
    private Path certPath;

    @JsonProperty(SOCKET_TIMEOUT)
    private Integer socketTimeout;

    @JsonProperty(CONNECT_TIMEOUT)
    private Integer connectTimeout;

    @JsonProperty(INSECURE)
    private boolean insecure;

    @JsonProperty(AWS_SIGV4)
    @JsonPropertyDescription("A Boolean flag used to sign the HTTP request with AWS credentials. " +
            "Only applies to Amazon OpenSearch Service. See <a href=\"https://github.com/opensearch-project/data-prepper/blob/129524227779ee35a327c27c3098d550d7256df1/data-prepper-plugins/opensearch/security.md\">OpenSearch security</a> for details.")
    private boolean awsSigv4;

    @JsonProperty(AWS_REGION)
    @JsonPropertyDescription("A string that represents the AWS Region of the Amazon OpenSearch Service domain, " +
            "for example, <code>us-west-2</code>. Only applies to Amazon OpenSearch Service.")
    private String awsRegion = DEFAULT_AWS_REGION;

    @JsonProperty(AWS_STS_ROLE_ARN)
    @JsonPropertyDescription("An AWS Identity and Access Management (IAM) role that the sink plugin assumes to sign the request to Amazon OpenSearch Service. " +
            "If not provided, the plugin uses the <a href=\"https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.html\">default credentials</a>.")
    @Size(max = 2048, message = "aws_sts_role_arn length cannot exceed 2048")
    private String awsStsRoleArn;

    @JsonProperty(AWS_OPTION)
    @JsonPropertyDescription("The <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/sinks/opensearch/#aws\">AWS</a> configuration.")
    private AwsOption awsOption;

    @JsonProperty(AWS_STS_EXTERNAL_ID)
    @Size(max = 1224, message = "aws_sts_external_id length cannot exceed 1224")
    @JsonPropertyDescription("The external ID to attach to AssumeRole requests from AWS STS.")
    private String awsStsExternalId;

    @JsonProperty(AWS_STS_HEADER_OVERRIDES)
    @JsonPropertyDescription("A map of header overrides that the IAM role assumes for the plugin.")
    private Map<String, String> awsStsHeaderOverrides;

    @JsonProperty(PROXY)
    @JsonPropertyDescription("The address of the <a href=\"https://en.wikipedia.org/wiki/Proxy_server\">forward HTTP proxy server</a>. " +
            "The format is \"&lt;hostname or IP&gt;:&lt;port&gt;\" (for example, \"example.com:8100\", \"http://example.com:8100\", \"112.112.112.112:8100\"). " +
            "The port number cannot be omitted.")
    private String proxy;

    @JsonProperty(AUTHENTICATION)
    @JsonPropertyDescription("The basic authentication configuration.")
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
