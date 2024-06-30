package com.example.jojoldu.unittest;

import com.example.jojoldu.entity.SalesSum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcPagingItemReader;
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
public class BatchOnlyJdbcReaderConfiguration {

    public static final DateTimeFormatter FORMATTER = ofPattern("yyyy-MM-dd");
    public static final String JOB_NAME = "batchOnlyJdbcReaderJob";

    private final DataSource dataSource;

    public BatchOnlyJdbcReaderConfiguration(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private int chunkSize;

    @Value("${chunkSize:1000}")
    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Bean
    @StepScope
    public JdbcPagingItemReader<SalesSum> batchOnlyJdbcReaderJobReader(
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
                .name("batchOnlyJdbcReaderJobReader")
                .pageSize(chunkSize)
                .fetchSize(chunkSize)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(SalesSum.class))
                .queryProvider(queryProvider.getObject())
                .parameterValues(params)
                .build();
    }
}
