package com.example.jojoldu.flow;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class StepNextConditionalJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public StepNextConditionalJobConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Job stepNextConditionalJob(Step conditionalJobStep1,
                                      Step conditionalJobStep2,
                                      Step conditionalJobStep3) {
        return jobBuilderFactory.get("stepNextConditionalJob")
                .start(conditionalJobStep1)
                    .on("FAILED") // FAILED 인 경우
                    .to(conditionalJobStep3)
                    .on("*") // step3 결과 관계없이
                    .end() // flow 종료
                .from(conditionalJobStep1) // step1로부터
                    .on("*") // FAILED 외에 모든 경우에 대해
                    .to(conditionalJobStep2)
                    .next(conditionalJobStep3)
                    .on("*") // step3 결과 관계없이
                    .end() // flow 종료
                .end() // job 종료
                .build();

    }

    @Bean
    @JobScope
    public Step conditionalJobStep1() {
        return stepBuilderFactory.get("conditionalJobStep1")
                .tasklet((contribution, chunkContext) -> {
                    log.info(">>> This is stepNextConditionalJob Step1");

                    // ExitStatus.FAILED 설정하여 FLOW 흐름 제어
                    //contribution.setExitStatus(ExitStatus.FAILED);

                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    @JobScope
    public Step conditionalJobStep2() {
        return stepBuilderFactory.get("conditionalJobStep2")
                .tasklet((contribution, chunkContext) -> {
                    log.info(">>> This is stepNextConditionalJob Step2");
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    @JobScope
    public Step conditionalJobStep3() {
        return stepBuilderFactory.get("conditionalJobStep3")
                .tasklet((contribution, chunkContext) -> {
                    log.info(">>> This is stepNextConditionalJob Step3");

                    return RepeatStatus.FINISHED;
                }).build();
    }
}
