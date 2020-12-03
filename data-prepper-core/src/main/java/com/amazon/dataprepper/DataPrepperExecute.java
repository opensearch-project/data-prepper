package com.amazon.dataprepper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execute entry into Data Prepper.
 */
public class DataPrepperExecute {
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperExecute.class);

    public static void main(String[] args) {
        final DataPrepper dataPrepper = DataPrepper.getInstance();
        if (args.length > 0) {
            dataPrepper.execute(args[0]);
        } else {
            LOG.error("Configuration file is required");
            System.exit(1);
        }
    }
}