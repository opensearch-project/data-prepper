package org.opensearch.dataprepper.plugins.aws.sqs.common.model;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class SqsOptionsTest {

    @Test
    void sqs_options_test(){
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/123099425585/myQueue";
        SqsOptions options = new SqsOptions.Builder().setSqsUrl(queueUrl)
                .setMaximumMessages(10).setPollDelay(Duration.ZERO).build();

        assertThat(options.getSqsUrl(),equalTo(queueUrl));
        assertThat(options.getMaximumMessages(),equalTo(10));
        assertThat(options.getPollDelay(),equalTo(Duration.ZERO));
    }

}
