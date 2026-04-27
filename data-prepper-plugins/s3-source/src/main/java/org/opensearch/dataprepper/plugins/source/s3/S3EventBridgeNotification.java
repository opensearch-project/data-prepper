package org.opensearch.dataprepper.plugins.source.s3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.joda.time.DateTime;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

import java.util.List;

/**
 * A helper class that represents a strongly typed notification item sent to EventBridge
 */
public class S3EventBridgeNotification {
    private final String version;
    private final String id;
    private final String detailType;
    private String source;
    private final String account;
    private DateTime time;
    private final String region;
    private final List<String> resources;
    private final Detail detail;

    @JsonCreator
    public S3EventBridgeNotification(
            @JsonProperty("version") final String version,
            @JsonProperty("id") final String id,
            @JsonProperty(value = "detail-type") final String detailType,
            @JsonProperty(value = "source", required = true) final String source,
            @JsonProperty(value = "account", required = true) final String account,
            @JsonProperty(value = "time", required = true) final String time,
            @JsonProperty(value = "region", required = true) final String region,
            @JsonProperty(value = "resources", required = true) final List<String> resources,
            @JsonProperty(value = "detail", required = true) final Detail detail) {
        this.version = version;
        this.id = id;
        this.detailType = detailType;
        this.source = source;
        this.account = account;
        if (time != null)
            this.time = DateTime.parse(time);
        this.region = region;
        this.resources = resources;
        this.detail = detail;
    }

    public String getVersion() {
        return version;
    }

    public String getId() {
        return id;
    }

    public String getDetailType() {
        return detailType;
    }

    public String getAccount() {
        return account;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @JsonSerialize(using=DateTimeJsonSerializer.class)
    public DateTime getTime() {
        return time;
    }

    public String getRegion() {
        return region;
    }

    public List<String> getResources() {
        return resources;
    }

    public Detail getDetail() {
        return detail;
    }

    public static class Detail {
        private final String version;
        private final Bucket bucket;
        private final Object object;
        private final String requestId;
        private final String requester;
        private final String sourceIpAddress;
        private final String reason;

        @JsonCreator
        public Detail(@JsonProperty(value = "version") final String version,
                      @JsonProperty(value = "bucket", required = true) final Bucket bucket,
                      @JsonProperty(value = "object", required = true) final Object object,
                      @JsonProperty("request-id") final String requestId,
                      @JsonProperty("requester") final String requester,
                      @JsonProperty("source-ip-address") final String sourceIpAddress,
                      @JsonProperty("reason") final String reason) {
            this.version = version;
            this.bucket = bucket;
            this.object = object;
            this.requestId = requestId;
            this.requester = requester;
            this.sourceIpAddress = sourceIpAddress;
            this.reason = reason;
        }

        public String getVersion() {
            return version;
        }

        public Object getObject() {
            return object;
        }

        public Bucket getBucket() {
            return bucket;
        }

        public String getRequestId() {
            return requestId;
        }

        public String getRequester() {
            return requester;
        }

        public String getSourceIpAddress() {
            return sourceIpAddress;
        }

        public String getReason() {
            return reason;
        }
    }

    public static class Bucket {
        private final String name;

        @JsonCreator
        public Bucket(@JsonProperty(value = "name", required = true) final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static class Object {
        private final String key;
        private final long size;
        private final String etag;
        private final String versionId;
        private final String sequencer;

        public Object(@JsonProperty(value = "key") final String key,
                      @JsonProperty(value = "size") final long size,
                      @JsonProperty(value = "etag") final String etag,
                      @JsonProperty(value = "version-id") final String versionId,
                      @JsonProperty(value = "sequencer") final String sequencer) {
            this.key = key;
            this.size = size;
            this.etag = etag;
            this.versionId = versionId;
            this.sequencer = sequencer;
        }

        public long getSize() {
            return size;
        }

        public String getEtag() {
            return etag;
        }

        public String getVersionId() {
            return versionId;
        }

        public String getSequencer() {
            return sequencer;
        }

        public String getUrlDecodedKey() {
            return SdkHttpUtils.urlDecode(key);
        }
    }
}
