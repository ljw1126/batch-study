package com.example.jojoldu.unittest;

import com.example.jojoldu.entity.SalesSum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Hashtable;
import java.util.Map;

import static java.time.format.DateTimeFormatter.*;

@Slf4j
@Configuration
public class BatchJpaTestConfiguration {

    public static final DateTimeFormatter FORMATTER = ofPattern("yyyy-MM-dd");
    public static final String JOB_NAME = "batchJpaUnitTestJob";

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory emf;

    @Value("${chunkSize:1000}")
    private int chunkSize;

    public BatchJpaTestConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, EntityManagerFactory emf) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.emf = emf;
    }

    @Bean
    public Job batchJpaUnitTestJob(Step batchJpaUnitTestStep) {
        return jobBuilderFactory.get(JOB_NAME)
                .start(batchJpaUnitTestStep)
                .build();
    }

    @Bean
    @JobScope
    public Step batchJpaUnitTestStep(JpaPagingItemReader<SalesSum> batchJpaUnitTestReader,
                                     JpaItemWriter<SalesSum> batchJpaUnitTestWriter) {
        return stepBuilderFactory.get("batchJpaUnitTestStep")
                .<SalesSum, SalesSum>chunk(chunkSize)
                .reader(batchJpaUnitTestReader(null))
                .writer(batchJpaUnitTestWriter)
                .build();
    }

    @Bean
    @StepScope
    public JpaPagingItemReader<SalesSum> batchJpaUnitTestReader(
            @Value("#{jobParameters[orderDate]}") String orderDate
    ) {
        Map<String, Object> params = new Hashtable<>();
        params.put("orderDate", LocalDate.parse(orderDate, FORMATTER));

        // HQL
        String queryString = "SELECT new com.example.jojoldu.entity.SalesSum(s.orderDate, SUM(s.amount)) " +
                        "FROM Sales s " +
                        "WHERE s.orderDate =:orderDate " +
                        "GROUP BY s.orderDate ";

        return new JpaPagingItemReaderBuilder<SalesSum>()
                .name("batchJpaUnitTestReader")
                .entityManagerFactory(emf)
                .pageSize(chunkSize)
                .queryString(queryString)
                .parameterValues(params)
                .build();
    }

    @Bean
    public JpaItemWriter<SalesSum> batchJpaUnitTestWriter() {
        return new JpaItemWriterBuilder<SalesSum>()
                .entityManagerFactory(emf)
                .build();
    }
}
