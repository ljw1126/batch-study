package com.example.job.parallel;

import com.example.dto.AmoutDto;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.PathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import java.io.File;
import java.io.IOException;

/**
 * 단일 프로세스에서 청크 단위로 병렬 처리한다
 */
@Configuration
public class MultiThreadStepJobConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public MultiThreadStepJobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Job multiThreadStepJob(Step multiThreadStep) {
        return jobBuilderFactory.get("multiThreadStepJob")
                .incrementer(new RunIdIncrementer())
                .start(multiThreadStep)
                .build();
    }

    @JobScope
    @Bean
    public Step multiThreadStep(FlatFileItemReader<AmoutDto> amountFileItemReader,
                                ItemProcessor<AmoutDto, AmoutDto> amountFileItemProcessor,
                                FlatFileItemWriter<AmoutDto> amountFileItemWriter,
                                TaskExecutor taskExecutor) {
        return stepBuilderFactory.get("multiThreadStep")
                .<AmoutDto, AmoutDto>chunk(10)
                .reader(amountFileItemReader)
                .processor(amountFileItemProcessor)
                .writer(amountFileItemWriter)
                .taskExecutor(taskExecutor)
                .build();
    }

    // 멀티스레드 실행
    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor("spring-batch-test-executor");
    }

    @Bean
    @StepScope
    public FlatFileItemReader<AmoutDto> amountFileItemReader() {
        return new FlatFileItemReaderBuilder<AmoutDto>()
                .name("amountFileItemReader")
                .fieldSetMapper(new AmountFieldSetMapper())
                .lineTokenizer(new DelimitedLineTokenizer(DelimitedLineTokenizer.DELIMITER_TAB))
                .resource(new ClassPathResource("samples/amount.txt"))
                .build();
    }

    @Bean
    @StepScope
    public ItemProcessor<AmoutDto, AmoutDto> amountFileItemProcessor() {
        return item -> {
            System.out.println(item + "\t" + Thread.currentThread().getName());

            return AmoutDto.builder()
                    .index(item.getIndex())
                    .name(item.getName())
                    .amount(item.getAmount() * 100)
                    .build();
        };
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<AmoutDto> amountFileItemWriter() throws IOException {
        BeanWrapperFieldExtractor<AmoutDto> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"index", "name", "amount"});
        fieldExtractor.afterPropertiesSet();

        DelimitedLineAggregator<AmoutDto> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setFieldExtractor(fieldExtractor);

        // 파일 생성, 덮어씌우기
        String filePath = "src/main/resources/samples/amount-output.txt";
        new File(filePath).createNewFile();
        FileSystemResource resource = new FileSystemResource(filePath);

        return new FlatFileItemWriterBuilder<AmoutDto>()
                .name("amountFileItemWriter")
                .resource(new PathResource(""))
                .lineAggregator(lineAggregator)
                .resource(resource)
                .build();
    }
}
