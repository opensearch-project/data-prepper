package com.amazon.dataprepper.parser.config;

import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.inject.Named;

@Configuration
public class DataPrepperConfigurationConfiguration {

    @Bean
    public DataPrepperConfiguration dataPrepperDefaultConfiguration(ApplicationArguments args) {
        return new DataPrepperConfiguration();
    }
}
