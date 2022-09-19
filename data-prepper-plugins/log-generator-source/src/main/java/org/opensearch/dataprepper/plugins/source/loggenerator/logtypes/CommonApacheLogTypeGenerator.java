/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loggenerator.logtypes;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.plugins.source.loggenerator.ApacheLogFaker;
import org.opensearch.dataprepper.plugins.source.loggenerator.LogTypeGenerator;

import java.util.Collections;

@DataPrepperPlugin(name = "apache_clf", pluginType = LogTypeGenerator.class)
public class CommonApacheLogTypeGenerator implements LogTypeGenerator {

    private static final ApacheLogFaker apacheLogFaker = new ApacheLogFaker();
    private static final String MESSAGE_FIELD_NAME = "message";

    @Override
    public Event generateEvent() {

        final String apacheLogString = apacheLogFaker.generateRandomCommonApacheLog();

        return JacksonLog.builder()
                .withData(Collections.singletonMap(MESSAGE_FIELD_NAME, apacheLogString))
                .build();
    }
}
