/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.plugins.kafka.configuration.CommonTopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.ConsumerTopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaKeyMode;
import org.opensearch.dataprepper.plugins.kafka.configuration.KmsConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;

class SourceTopicConfig extends CommonTopicConfig implements ConsumerTopicConfig {
    static final Duration DEFAULT_COMMIT_INTERVAL = Duration.ofSeconds(5);

    @JsonProperty("serde_format")
    private MessageFormat serdeFormat = MessageFormat.PLAINTEXT;

    @JsonProperty("commit_interval")
    @Valid
    @Size(min = 1)
    private Duration commitInterval = DEFAULT_COMMIT_INTERVAL;

    @JsonProperty("key_mode")
    private KafkaKeyMode kafkaKeyMode = KafkaKeyMode.INCLUDE_AS_FIELD;

    @Override
    public String getEncryptionKey() {
        return null;
    }

    @Override
    public KmsConfig getKmsConfig() {
        return null;
    }


    @Override
    public Duration getCommitInterval() {
        return commitInterval;
    }


    public KafkaKeyMode getKafkaKeyMode() {
        return kafkaKeyMode;
    }

    public MessageFormat getSerdeFormat() {
        return serdeFormat;
    }
}
