package org.opensearch.dataprepper.plugins.source.oteltelemetry;

public class OTelTelemetrySourceConfig {
    private int port = 21892;
    private long requestTimeout = 10000;
    private boolean sslEnabled = false;
    private String sslKeyCertChainFile;
    private String sslKeyFile;
    private boolean healthCheckServiceEnabled = true;
    private String logsPath = "/v1/logs";
    private String metricsPath = "/v1/metrics";
    private String tracesPath = "/v1/traces";

    public int getPort() {
        return port;
    }

    public long getRequestTimeout() {
        return requestTimeout;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public String getSslKeyCertChainFile() {
        return sslKeyCertChainFile;
    }

    public String getSslKeyFile() {
        return sslKeyFile;
    }

    public boolean isHealthCheckServiceEnabled() {
        return healthCheckServiceEnabled;
    }

    public String getLogsPath() {
        return logsPath;
    }

    public String getMetricsPath() {
        return metricsPath;
    }

    public String getTracesPath() {
        return tracesPath;
    }

    public boolean useAcmCertForSSL() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'useAcmCertForSSL'");
    }

    public long getAcmCertIssueTimeOutMillis() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getAcmCertIssueTimeOutMillis'");
    }

    public String getAwsRegion() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getAwsRegion'");
    }

    public boolean isSslCertAndKeyFileInS3() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isSslCertAndKeyFileInS3'");
    }

    public String getAcmPrivateKeyPassword() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getAcmPrivateKeyPassword'");
    }

    public String getAcmCertificateArn() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getAcmCertificateArn'");
    }
}
