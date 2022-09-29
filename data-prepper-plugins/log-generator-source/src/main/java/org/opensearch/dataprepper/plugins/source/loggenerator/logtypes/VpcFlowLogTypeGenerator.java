/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loggenerator.logtypes;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.plugins.source.loggenerator.CsvLogFaker;
import org.opensearch.dataprepper.plugins.source.loggenerator.LogTypeGenerator;

import java.util.Collections;

@DataPrepperPlugin(name = "vpc_flow", pluginType = LogTypeGenerator.class)
public class VpcFlowLogTypeGenerator implements LogTypeGenerator {
    private static final String MESSAGE_FIELD_NAME = "message";
    private static final CsvLogFaker csvLogFaker = new CsvLogFaker();

    @Override
    public Event generateEvent() {
        final String vpcFlowLogString = csvLogFaker.generateRandomStandardVPCFlowLog();

        return JacksonLog.builder()
                .withData(Collections.singletonMap(MESSAGE_FIELD_NAME, vpcFlowLogString))
                .build();
    }
}
