package com.example.jojoldu.parallel;

import com.example.jojoldu.entity.product.Product;
import com.example.jojoldu.entity.product.ProductBackup;
import com.example.jojoldu.entity.product.ProductBackupRepository;
import com.example.jojoldu.entity.product.ProductRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.persistence.EntityManagerFactory;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * 기술 블로그
 * https://jojoldu.tistory.com/550
 *
 * 깃허브
 * https://github.com/jojoldu/spring-batch-in-action/blob/master/src/main/java/com/jojoldu/batch/example/partition/PartitionLocalConfiguration.java#L40
 *
 * Partitioner - StepExecution 생성
 * Splitter - StepExecution을 분할하려 파티셔닝 생성
 * PartitionHandler - 파티션 실행하는 역할 (내부에서 splitter 실행후 파티션 실행함)
 */
@Slf4j
@Configuration
public class PartitionLocalConfiguration {
    private static final String JOB_NAME = "partitionLocalBatch";

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;
    private final ProductRepository productRepository;
    private final ProductBackupRepository productBackupRepository;

    public PartitionLocalConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, EntityManagerFactory entityManagerFactory, ProductRepository productRepository, ProductBackupRepository productBackupRepository) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.entityManagerFactory = entityManagerFactory;
        this.productRepository = productRepository;
        this.productBackupRepository = productBackupRepository;
    }

    private int chunkSize;

    @Value("${chunkSize:100}")
    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    private int poolSize;

    @Value("${poolSize:5}")
    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    @Bean(name = JOB_NAME + "_job")
    public Job job() {
        return jobBuilderFactory.get(JOB_NAME + "_job")
                .start(step1Manager())
                .preventRestart()
                .build();
    }

    @Bean(name = JOB_NAME + "_step1Manager")
    public Step step1Manager() {
        return stepBuilderFactory.get(JOB_NAME + "_step1Manager")
                .partitioner("step1", partitioner(null, null)) // step1에서 사용될 Partitioner 구현체 등록
                .step(step1())// 파티셔닝될 step 등록, Partitioner 로직에 따라 서로 다른 StepExecution을 가진 여러 slave step으로 파티셔닝된다
                .partitionHandler(partitionHandler())
                .build();
    }

    // 각 worker step이 어떤 Step Executions 변수를 가지게 할지 결정, Worker Step 수 결정
    @Bean(name = JOB_NAME + "_partitioner")
    @StepScope
    public ProductIdRangePartitioner partitioner(
            @Value("#{jobParameters['startDate']}") String startDate,
            @Value("#{jobParameters['endDate']}") String endDate
    ) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate startLocalDate = LocalDate.parse(startDate, dateTimeFormatter);
        LocalDate endLocalDate = LocalDate.parse(endDate, dateTimeFormatter);

        return new ProductIdRangePartitioner(productRepository, startLocalDate, endLocalDate);
    }

    @Bean(name = JOB_NAME + "_taskPool")
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize);
        executor.setThreadNamePrefix("partition-thread");
        executor.setWaitForTasksToCompleteOnShutdown(Boolean.TRUE);
        executor.initialize();
        return executor;
    }

    @Bean(name = JOB_NAME + "_partitionHandler")
    public TaskExecutorPartitionHandler partitionHandler() {
        TaskExecutorPartitionHandler partitionHandler = new TaskExecutorPartitionHandler();
        partitionHandler.setStep(step1());
        partitionHandler.setTaskExecutor(taskExecutor());
        partitionHandler.setGridSize(poolSize); // 스레드 개수(slave worker)와 poolSize를 맞춤
        return partitionHandler;
    }

    @Bean(name = JOB_NAME + "_step1")
    public Step step1() {
        return stepBuilderFactory.get(JOB_NAME + "_step")
                .<Product, ProductBackup>chunk(chunkSize)
                .reader(reader(null, null))
                .processor(processor())
                .writer(writer(null, null))
                .build();
    }

    @Bean(name = JOB_NAME + "_reader")
    @StepScope
    public JpaPagingItemReader<Product> reader(
            @Value("#{stepExecutionContext[minId]}") Long minId,
            @Value("#{stepExecutionContext[maxId]}") Long maxId
    ) {

        Map<String, Object> params = new HashMap<>();
        params.put("minId", minId);
        params.put("maxId", maxId);

        log.info("reader minId = {}, maxId = {}", minId, maxId);

        return new JpaPagingItemReaderBuilder<Product>()
                .name(JOB_NAME + "_reader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(chunkSize)
                .queryString("SELECT p FROM Product p WHERE p.id BETWEEN :minId AND :maxId")
                .parameterValues(params)
                .build();
    }


    @Bean(name = JOB_NAME + "_processor")
    public ItemProcessor<Product, ProductBackup> processor() {
        return ProductBackup::new;
    }


    @Bean(name = JOB_NAME + "_writer")
    @StepScope
    public ItemWriter<ProductBackup> writer(
            @Value("#{stepExecutionContext[minId]}") Long minId,
            @Value("#{stepExecutionContext[maxId]}") Long maxId
    ) {
        return items -> {
            log.info("Writer stepExecutionContext minId={}, current minId={}", minId, items.get(0).getOriginId());
            log.info("Writer stepExecutionContext maxId={}, current maxId={}", maxId, items.get(items.size()-1).getOriginId());

            productBackupRepository.saveAll(items);
        };
    }
}
