package com.example.jojoldu.unittest;

import com.example.jojoldu.entity.SalesSum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Hashtable;
import java.util.Map;

import static java.time.format.DateTimeFormatter.*;

@Slf4j
@Configuration
public class BatchJdbcTestConfiguration {

    public static final DateTimeFormatter FORMATTER = ofPattern("yyyy-MM-dd");
    public static final String JOB_NAME = "batchJdbcUnitTestJob";

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    public BatchJdbcTestConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.dataSource = dataSource;
    }

    private int chunkSize;

    @Value("${chunkSize:1000}")
    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Bean
    public Job batchJdbcUnitTestJob(Step batchJdbcUnitTestJobStep) {
        return jobBuilderFactory.get(JOB_NAME)
                .incrementer(new RunIdIncrementer())
                .start(batchJdbcUnitTestJobStep)
                .build();
    }


    @Bean
    @JobScope
    public Step batchJdbcUnitTestJobStep() throws Exception {
        return stepBuilderFactory.get("batchJdbcUnitTestJobStep")
                .<SalesSum, SalesSum>chunk(chunkSize)
                .reader(batchJdbcUnitTestJobReader(null))
                .writer(batchJdbcUnitTestJobWriter())
                .build();
    }

    @Bean
    @StepScope
    public JdbcPagingItemReader<SalesSum> batchJdbcUnitTestJobReader(
            @Value("#{jobParameters[orderDate]}") String orderDate
    ) throws Exception {
        Map<String, Object> params = new Hashtable<>();
        params.put("orderDate", LocalDate.parse(orderDate, FORMATTER));

        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(dataSource);
        queryProvider.setSelectClause("order_date, sum(amount) as amount_sum ");
        queryProvider.setFromClause("from sales ");
        queryProvider.setWhereClause("where order_date =:orderDate ");
        queryProvider.setGroupClause("group by order_date ");
        queryProvider.setSortKey("order_date"); // ASCENDING 오름차순

        return new JdbcPagingItemReaderBuilder<SalesSum>()
                .name("batchJdbcUnitTestJobReader")
                .pageSize(chunkSize)
                .fetchSize(chunkSize)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(SalesSum.class))
                .queryProvider(queryProvider.getObject())
                .parameterValues(params)
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<SalesSum> batchJdbcUnitTestJobWriter() {
        return new JdbcBatchItemWriterBuilder<SalesSum>()
                .dataSource(dataSource)
                .sql("insert into sales_sum(order_date, amount_aum) values(:order_date, :amount_sum)")
                .beanMapped()
                .build();
    }
}
