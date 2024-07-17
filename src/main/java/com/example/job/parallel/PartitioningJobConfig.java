package com.example.job.parallel;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.SimplePartitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;

/**
 * 단일 프로세스에서 마스터 스텝과 워크 스텝을 두고, 마스터 스텝에서 생성해준 파티션 단위로 스텝을 병렬처리한다
 */
@Configuration
public class PartitioningJobConfig {

    private static final int PARTITION_SIZE = 100;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public PartitioningJobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Job partitioningJob(Step mastetStep) {
        return jobBuilderFactory.get("partitioningJob")
                .incrementer(new RunIdIncrementer())
                .start(mastetStep)
                .build();
    }

    @Bean
    @JobScope
    public Step mastetStep(Partitioner partitioner, TaskExecutorPartitionHandler partitionHandler) {
        return stepBuilderFactory.get("mastetStep")
                .partitioner("anotherStep", partitioner)
                .partitionHandler(partitionHandler)
                .build();
    }

    @StepScope
    @Bean
    public Partitioner partitioner() {
        SimplePartitioner partitioner = new SimplePartitioner();
        partitioner.partition(PARTITION_SIZE);
        return partitioner;
    }

    @Bean
    @StepScope
    public TaskExecutorPartitionHandler partitionHandler(Step anotherStep, TaskExecutor taskExecutor) {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(anotherStep);
        handler.setGridSize(PARTITION_SIZE);
        handler.setTaskExecutor(taskExecutor);
        return handler;
    }
}
