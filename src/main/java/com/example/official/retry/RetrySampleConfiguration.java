package com.example.official.retry;

import com.example.official.reader.GeneratingTradeItemReader;
import com.example.official.writer.RetrySampleItemWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//https://github.com/spring-projects/spring-batch/blob/main/spring-batch-samples/src/test/java/org/springframework/batch/samples/retry/RetrySampleFunctionalTests.java
@Slf4j
@Configuration
public class RetrySampleConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public RetrySampleConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Job retrySampleJob(Step retryStep) {
        return jobBuilderFactory.get("retrySampleJob")
                .start(retryStep)
                .build();
    }

    // retry는 processor와 writer에만 적용된다
    @Bean
    @JobScope
    protected Step retryStep(GeneratingTradeItemReader reader, RetrySampleItemWriter<Object> writer) {
        return stepBuilderFactory.get("retryStep")
                .chunk(1)
                .reader(reader)
                .writer(writer)
                .faultTolerant()
                .retry(Exception.class)
                .retryLimit(3)
                .build();
    }

    @Bean
    @StepScope
    protected GeneratingTradeItemReader reader() {
        GeneratingTradeItemReader reader = new GeneratingTradeItemReader();
        reader.setLimit(10);
        return reader;
    }

    @Bean
    @StepScope
    protected RetrySampleItemWriter<Object> writer() {
        return new RetrySampleItemWriter<>();
    }
}
