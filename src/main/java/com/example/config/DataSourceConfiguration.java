package com.example.config;

import com.zaxxer.hikari.HikariDataSource;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@RequiredArgsConstructor
@Configuration
public class DataSourceConfiguration {
    private static final String PROPERTIES = "spring.datasource.hikari";

    public static final String MASTER_DATASOURCE = "dataSource";
    public static final String READER_DATASOURCE = "readerDataSource";

    @Bean(MASTER_DATASOURCE)
    @Primary
    @ConfigurationProperties(prefix = PROPERTIES)
    public DataSource dataSource() {
        return DataSourceBuilder.create()
                .type(HikariDataSource.class)
                .build();
    }

    @Bean(READER_DATASOURCE)
    @ConfigurationProperties(prefix = PROPERTIES)
    public DataSource readerDataSource() {
        HikariDataSource hikariDataSource = DataSourceBuilder.create()
                .type(HikariDataSource.class)
                .build();
        hikariDataSource.setReadOnly(true);
        return hikariDataSource;
    }
}
