package com.amazon.situp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execute entry into SITUP.
 */
public class SitupExecute {
    private static final Logger LOG = LoggerFactory.getLogger(SitupExecute.class);

    public static void main(String[] args) {
        final Situp situp = Situp.getInstance();
        if (args.length > 0) {
            situp.execute(args[0]);
        } else {
            LOG.error("Configuration file is required");
            System.exit(1);
        }
    }
}