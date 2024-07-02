package com.example.jojoldu.unittest;

import com.example.jojoldu.TestH2DataSourceConfiguration;
import com.example.jojoldu.entity.SalesSum;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.time.LocalDate;

import static com.example.jojoldu.unittest.BatchOnlyJdbcReaderConfiguration.FORMATTER;
import static org.assertj.core.api.Assertions.assertThat;

class BatchOnlyJdbcReaderConfigurationTest {
    private DataSource dataSource;
    private JdbcTemplate jdbcTemplate;
    private ConfigurableApplicationContext context;
    private LocalDate orderDate;
    private BatchOnlyJdbcReaderConfiguration job;

    @BeforeEach
    void setUp() {
        this.context = new AnnotationConfigApplicationContext(TestH2DataSourceConfiguration.class);
        this.dataSource = (DataSource) context.getBean("dataSource");
        this.jdbcTemplate = new JdbcTemplate(this.dataSource);

        this.orderDate = LocalDate.of(2024, 6, 30);
        this.job = new BatchOnlyJdbcReaderConfiguration(dataSource);
        this.job.setChunkSize(10);
    }

    @AfterEach
    void tearDown() {
        if(this.context != null) {
            this.context.close();
        }
    }

    @Test
    void 기간내_Sales가_집계되어_SalesSum이된다() throws Exception {
        long amount1 = 1000;
        long amount2 = 100;
        long amount3 = 10;
        String sql = "insert into sales(order_date, amount, order_no) values(?, ?, ?)";
        jdbcTemplate.update(sql, orderDate, amount1, "1");
        jdbcTemplate.update(sql, orderDate, amount2, "2");
        jdbcTemplate.update(sql, orderDate, amount3, "3");

        JdbcPagingItemReader<SalesSum> reader = job.batchOnlyJdbcReaderJobReader(orderDate.format(FORMATTER));
        reader.afterPropertiesSet(); // Reader 쿼리 생성

        assertThat(reader.read().getAmountSum()).isEqualTo(amount1 + amount2 + amount3);
        assertThat(reader.read()).isNull();
    }

}
