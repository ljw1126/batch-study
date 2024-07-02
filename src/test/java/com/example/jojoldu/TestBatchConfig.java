package com.example.jojoldu;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

// BatchJpaConfiguration, DataSourceConfiguration, EntitymanagerFactoryCreator
@Configuration
@EnableAutoConfiguration
@EnableBatchProcessing
@ComponentScan(basePackages = "com.example.config")
public class TestBatchConfig {
}
