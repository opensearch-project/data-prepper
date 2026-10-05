/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.dlq.CloudWatchLogsSinkDlqData;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CloudWatchLogsSinkUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsSinkUtils.class);

    /**
     * Whether a configured value is a Data Prepper format expression carrying a {@code ${...}} field
     * reference or expression, as opposed to a literal. Dynamic values are resolved per request rather than
     * applied once at client build.
     */
    public static boolean isDynamicExpression(final String value, final ExpressionEvaluator expressionEvaluator) {
        if (value == null) {
            return false;
        }
        return !expressionEvaluator.extractDynamicKeysFromFormatExpression(value).isEmpty()
                || !expressionEvaluator.extractDynamicExpressionsFromFormatExpression(value).isEmpty();
    }

    public static DlqObject createDlqObject(final int status, final EventHandle eventHandle, final String message, final String failureMessage, final DlqPushHandler dlqPushHandler, final boolean dropIfDlqNotConfigured) {
        if (dlqPushHandler != null) {
            CloudWatchLogsSinkDlqData cloudWatchLogsSinkDlqData = CloudWatchLogsSinkDlqData.createDlqData(status, message, failureMessage);
            return DlqObject.createDlqObject(dlqPushHandler.getPluginSetting(), List.of(eventHandle), cloudWatchLogsSinkDlqData);
        } else {
            eventHandle.release(dropIfDlqNotConfigured);
        }
        return null;
    }

    public static void handleDlqObjects(List<DlqObject> dlqObjects, final DlqPushHandler dlqPushHandler) {
        if (dlqObjects.size() == 0) {
            return;
        }
        boolean result = false;
        if (dlqPushHandler != null) {
            result = dlqPushHandler.perform(dlqObjects);
        } 
        for (final DlqObject dlqObject : dlqObjects) {
            dlqObject.releaseEventHandles(result);
        }
    }
}


