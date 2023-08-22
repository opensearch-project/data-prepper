package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum NotificationSourceOption {
    // SQS and SNS with fan-out
    S3("s3"),
    EVENTBRIDGE("eventbridge");

    private static final Map<String, NotificationSourceOption> OPTIONS_MAP = Arrays.stream(NotificationSourceOption.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private final String option;

    NotificationSourceOption(final String option) {
        this.option = option.toLowerCase();
    }

    @JsonCreator
    static NotificationSourceOption fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option.toLowerCase());
    }
}
