/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loggenerator;

import java.util.Arrays;
import java.util.List;
import java.util.Random;


/**
 * A Log Generator that generates space-delimited Default VPC Flow logs.
 * See <a href="https://docs.aws.amazon.com/vpc/latest/userguide/flow-logs.html#flow-log-records">Standard VPC Flow Logs</a>
 */
public class CsvLogFaker {
    private static final int VPC_FLOW_LOGS_VERSION = 2;
    private static final String[] SRCPORT = new String[] {"20", "22", "80", "443"};
    private static final String[] ACTION = new String[] {"ACCEPT", "REJECT"};
    private static final String[] LOG_STATUS = new String[] {"OK", "NODATA", "SKIPDATA"};
    private final Random random = new Random();

    /**
     * Generate a single Standard VPC Flow log
     * @return Sample Standard VPC Flow Log
     */
    public String generateRandomStandardVPCFlowLog() {
        final String destinationPort = String.valueOf(49152 + random.nextInt((65535-49152))); // private ports range from 49152 to 65535
        final String packets = String.valueOf(random.nextInt(255));
        final int startTime = (int)System.currentTimeMillis();
        final int endTime = startTime + 1 + random.nextInt(999);
        final String bytes = String.valueOf(random.nextInt(1001) + 4000);
        final List<String> logAttributes = Arrays.asList(
                String.valueOf(VPC_FLOW_LOGS_VERSION),
                generateAccountId(),
                generateInterfaceId(),
                randomIpV4Address(),
                randomIpV4Address(),
                getRandomElementOf(SRCPORT),
                destinationPort,
                packets,
                bytes,
                String.valueOf(startTime),
                String.valueOf(endTime),
                getRandomElementOf(ACTION),
                getRandomElementOf(LOG_STATUS)
        );
        final String log = String.join(" ", logAttributes);
        return log;
    }

    private String generateInterfaceId() {
        final String prefix = "eni-";
        return prefix + generateRandomDigitString(16);
    }

    private String generateAccountId() {
        final int accountIdLength = 12;

        return generateRandomDigitString(accountIdLength);

    }
    private String generateRandomDigitString(final int length) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < length; ++i) {
            result.append(random.nextInt(10));
        }
        return result.toString();
    }
    private String getRandomElementOf(final String[] array) {
        return array[random.nextInt(array.length)];
    }
    private String randomIpV4Address() {
        return random.nextInt(255) + "." +
                random.nextInt(255) + "." +
                random.nextInt(255) + "." +
                random.nextInt(255);
    }
}
