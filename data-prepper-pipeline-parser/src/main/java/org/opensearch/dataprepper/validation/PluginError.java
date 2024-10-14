package org.opensearch.dataprepper.validation;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Getter
@Builder
public class PluginError {
    static final String PIPELINE_DELIMITER = ".";
    static final String PATH_TO_CAUSE_DELIMITER = ":";
    static final String CAUSED_BY_DELIMITER = " caused by: ";
    private final String pipelineName;
    private final String componentType;
    @NonNull
    private final String pluginName;
    @NonNull
    private final Exception exception;

    public String getErrorMessage() {
        final StringBuilder message = new StringBuilder();
        if (pipelineName != null) {
            message.append(pipelineName);
            message.append(PIPELINE_DELIMITER);
        }
        if (componentType != null) {
            message.append(componentType);
            message.append(PIPELINE_DELIMITER);
        }
        message.append(pluginName);
        message.append(PATH_TO_CAUSE_DELIMITER);
        message.append(getFlattenedExceptionMessage(CAUSED_BY_DELIMITER));
        return message.toString();
    }

    private String getFlattenedExceptionMessage(final String delimiter) {
        final StringBuilder message = new StringBuilder();
        Throwable throwable = exception;

        while (throwable != null) {
            if (throwable.getMessage() != null) {
                message.append(delimiter);
                message.append(throwable.getMessage());
            }
            throwable = throwable.getCause();
        }

        return message.toString();
    }
}
