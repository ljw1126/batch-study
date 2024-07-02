package com.example.jojoldu.unittest;

import com.example.jojoldu.TestH2DataSourceConfiguration;
import com.example.jojoldu.entity.SalesSum;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;

import javax.sql.DataSource;
import java.time.LocalDate;

import static com.example.jojoldu.unittest.BatchJdbcTestConfiguration.FORMATTER;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * 10.1 Spring Batch 단위 테스트 학습
 * https://jojoldu.tistory.com/456
 *
 * @EnableBatchProcessing 배치 환경 자동설정
 * @ContextConfiguration - 테스트 수행시 import할 config 클래스 등록
 */
@SpringBootTest
@SpringBatchTest
@EnableBatchProcessing
@ContextConfiguration(classes = {
        BatchJdbcTestConfiguration.class,
        TestH2DataSourceConfiguration.class
})
class BatchJdbcTestConfigurationTest {

    @Autowired
    private JdbcPagingItemReader<SalesSum> reader;

    @Autowired
    private DataSource dataSource;

    private JdbcOperations jdbcTemplate;
    private LocalDate orderDate = LocalDate.of(2024, 6, 30);

    public StepExecution getStepExecution() {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("orderDate", this.orderDate.format(FORMATTER))
                .toJobParameters();

        return MetaDataInstanceFactory.createStepExecution(jobParameters);
    }

    @BeforeEach
    void setUp() {
        this.reader.setDataSource(dataSource);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @AfterEach
    void tearDown() {
        this.jdbcTemplate.update("delete from sales");
    }

    @Test
    void 기간내_Sales가_집계되어_SalesSum이된다() throws Exception {
        long amount1 = 1000;
        long amount2 = 500;
        long amount3 = 100;

        saveSales(amount1, "1");
        saveSales(amount2, "2");
        saveSales(amount3, "3");

        assertThat(reader.read().getAmountSum()).isEqualTo(amount1 + amount2 + amount3);
        assertThat(reader.read()).isNull();
    }

    private void saveSales(long amount, String orderNo) {
        jdbcTemplate.update("insert into sales (order_date, amount, order_no) values (?, ?, ?)", this.orderDate, amount, orderNo);
    }
}
