package com.example.jojoldu.writer;

import com.example.jojoldu.entity.Pay;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.util.Map;

/**
 * 예제
 * https://github.com/jojoldu/spring-batch-in-action/blob/master/src/main/java/com/jojoldu/batch/example/writer/JdbcBatchItemWriterJobConfiguration.java
 */
@Slf4j
@Configuration
public class JdbcBatchItemWriterJobConfiguration {

    private static final int chunkSize = 10;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    public JdbcBatchItemWriterJobConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.dataSource = dataSource;
    }

    @Bean
    public Job jdbcBatchItemWriterJob(Step jdbcBatchItemWriterStep) {
        return jobBuilderFactory.get("jdbcBatchItemWriterJob")
                .start(jdbcBatchItemWriterStep)
                .build();
    }

    @Bean
    @JobScope
    public Step jdbcBatchItemWriterStep(JdbcCursorItemReader<Pay> jdbcBatchItemWriterReader,
                                        JdbcBatchItemWriter<Pay> jdbcBatchItemWriter) {
        return stepBuilderFactory.get("jdbcBatchItemWriterStep")
                .<Pay, Pay>chunk(chunkSize)
                .reader(jdbcBatchItemWriterReader)
                .writer(jdbcBatchItemWriter)
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<Pay> jdbcBatchItemWriterReader() {
        return new JdbcCursorItemReaderBuilder<Pay>()
                .name("jdbcBatchItemWriterReader")
                .dataSource(dataSource)
                .fetchSize(chunkSize)
                .rowMapper(new BeanPropertyRowMapper<>(Pay.class))
                .sql("SELECT id, amount, tx_name, tx_date_time FROM pay")
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Pay> jdbcBatchItemWriter() {
        return new JdbcBatchItemWriterBuilder<Pay>()
                .dataSource(this.dataSource)
                .sql("insert into pay(amount, tx_name, tx_date_time) values (:amount, :txName, :txDateTime)")
                .beanMapped()
                .build();
    }

    public JdbcBatchItemWriter<Map<String, Object>> jdbcBatchItemWriterByColumnMapped() {
        return new JdbcBatchItemWriterBuilder<Map<String, Object>>()
                .columnMapped()
                .sql("insert into pay(amount, tx_name, tx_date_time) values (:amount, :txName, :txDateTime)")
                .beanMapped()
                .build();
    }
}
