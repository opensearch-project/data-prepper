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
        boolean executeSubmissionStatus;
        if (args.length > 0) {
            executeSubmissionStatus = situp.execute(args[0]);
        } else {
            executeSubmissionStatus = situp.execute();
        }
        if (executeSubmissionStatus) {
            LOG.info("Submitted execution request successfully");
        } else {
            LOG.warn("Something went wrong - Failed to submit request");
        }
    }
}