/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils;

import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.dlq.CloudWatchLogsSinkDlqData;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CloudWatchLogsSinkUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchLogsSinkUtils.class);
    public static DlqObject createDlqObject(final int status, final EventHandle eventHandle, final String message, final String failureMessage, final DlqPushHandler dlqPushHandler) {
        if (dlqPushHandler != null) {
            CloudWatchLogsSinkDlqData cloudWatchLogsSinkDlqData = CloudWatchLogsSinkDlqData.createDlqData(status, message, failureMessage);
            return DlqObject.createDlqObject(dlqPushHandler.getPluginSetting(), eventHandle, cloudWatchLogsSinkDlqData);
        } else {
            eventHandle.release(false);
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
            dlqObject.getEventHandle().release(result);
        }
    }
}


