package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan("org.opensearch.dataprepper.expression")
public class TestExpressionSpringConfig {
    @Bean
    public EventKeyFactory eventKeyFactory() {
        return TestEventKeyFactory.getTestEventFactory();
    }
}
