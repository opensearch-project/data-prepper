package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.joda.time.DateTime;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

import java.util.List;

public class EventBridgeNotification {
    private String version;
    private String id;
    private String detailType;
    private String source;
    private String account;
    private DateTime time;
    private String region;
    private List<String> resources;
    private Detail detail;

    @JsonCreator
    public EventBridgeNotification(
            @JsonProperty("version") String version,
            @JsonProperty("id") String id,
            @JsonProperty("detail-type") String detailType,
            @JsonProperty("source") String source,
            @JsonProperty("account") String account,
            @JsonProperty("time") String time,
            @JsonProperty("region") String region,
            @JsonProperty("resources") List<String> resources,
            @JsonProperty("detail") Detail detail) {
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
        private String version;
        private Bucket bucket;
        private Object object;
        @JsonProperty("request-id")
        private String requestId;
        private String requester;
        @JsonProperty("source-ip-address")
        private String sourceIpAddress;
        private String reason;

        public Detail() {
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
        private String name;

        public Bucket() {
        }

        public String getName() {
            return name;
        }
    }

    public static class Object {
        private String key;
        private int size;
        private String etag;
        @JsonProperty("version-id")
        private String versionId;
        private String sequencer;

        public Object() {
        }

        public String getKey() {
            return key;
        }

        public int getSize() {
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
            return SdkHttpUtils.urlDecode(getKey());
        }
    }
}
