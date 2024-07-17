package com.example.job.parallel;

import com.example.dto.AmoutDto;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;

/**
 * 단일 프로세스 멀티 스레드에서 flow를 사용해 스텝을 동시에 실행한다
 */
@Configuration
public class ParallelStepJobConfig {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public ParallelStepJobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Job parallelJob(Flow splitFlow) {
        return jobBuilderFactory.get("parallelJob")
                .incrementer(new RunIdIncrementer())
                .start(splitFlow)
                .build()
                .build();
    }

    @Bean
    public Flow splitFlow(TaskExecutor taskExecutor,
                          Flow flowAmountFileStep,
                          Flow flowAnotherStep) {
        return new FlowBuilder<SimpleFlow>("splitFlow")
                .split(taskExecutor)
                .add(flowAmountFileStep, flowAnotherStep)
                .build();
    }

    @Bean
    public Flow flowAmountFileStep(Step amountFileStep) {
        return new FlowBuilder<SimpleFlow>("flowAmountFileStep")
                .start(amountFileStep)
                .build();
    }

    // MultiThreadStepJobConfig 에 Bean을 사용
    @Bean
    public Step amountFileStep(FlatFileItemReader<AmoutDto> amountFileItemReader,
                               ItemProcessor<AmoutDto, AmoutDto> amountFileItemProcessor,
                               FlatFileItemWriter<AmoutDto> amountFileItemWriter,
                               TaskExecutor taskExecutor) {
        return stepBuilderFactory.get("amountFileStep")
                .<AmoutDto, AmoutDto>chunk(10)
                .reader(amountFileItemReader)
                .processor(amountFileItemProcessor)
                .writer(amountFileItemWriter)
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    public Flow flowAnotherStep(Step anotherStep) {
        return new FlowBuilder<SimpleFlow>("flowAnotherStep")
                .start(anotherStep)
                .build();
    }

    @Bean
    public Step anotherStep() {
        return stepBuilderFactory.get("anotherStep")
                .tasklet((contribution, chunkContext) -> {
                    Thread.sleep(500); // 0.5 s
                    System.out.println("Another Step Completed, Thread = " + Thread.currentThread().getName());
                    return RepeatStatus.FINISHED;
                }).build();
    }


}
