package com.example.job.player;

import com.example.dto.PlayerDto;
import com.example.dto.PlayerSalaryDto;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.PathResource;

@Configuration
public class FlatFileJobConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public FlatFileJobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Job flatFileJob(Step flatFileStep) {
        return jobBuilderFactory.get("flatFileJob")
                .incrementer(new RunIdIncrementer())
                .start(flatFileStep)
                .build();
    }

    @Bean
    @JobScope
    public Step flatFileStep(FlatFileItemReader<PlayerDto> playerDtoFlatFileItemReader) {
        return stepBuilderFactory.get("flatFileStep")
                .<PlayerDto, PlayerDto>chunk(5)
                .reader(playerDtoFlatFileItemReader)
                .writer(items -> items.forEach(System.out::println))
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<PlayerDto> playerFileItemReader() {
        return new FlatFileItemReaderBuilder<PlayerDto>()
                .name("playerFileItemReader")
                .lineTokenizer(new DelimitedLineTokenizer()) // 콤마 구분
                .linesToSkip(1)
                .fieldSetMapper(new PlayerFieldSetMapper()) // mapper 구현체
                .resource(new ClassPathResource("samples/players.txt"))
                .build();
    }
}
