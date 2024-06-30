package com.example.jojoldu.unittest;

import com.example.jojoldu.entity.Sales;
import com.example.jojoldu.entity.SalesSum;
import com.example.jojoldu.entity.SalesRepository;
import com.example.jojoldu.entity.SalesSumRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDate;

import static com.example.jojoldu.unittest.BatchJdbcTestConfiguration.FORMATTER;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = {BatchJpaTestConfiguration.class, TestBatchConfig.class})
@SpringBatchTest
@ActiveProfiles("test")
class BatchJpaTestConfigurationTest {

    @Autowired
    private JpaPagingItemReader<SalesSum> reader;

    @Autowired
    private SalesRepository salesRepository;

    @Autowired
    private SalesSumRepository salesSumRepository;

    private static final LocalDate orderDate = LocalDate.of(2024,6,30);

    @AfterEach
    void tearDown() {
        salesRepository.deleteAllInBatch();
        salesSumRepository.deleteAllInBatch();
    }

    public StepExecution getStepExecution() {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("orderDate", this.orderDate.format(FORMATTER))
                .toJobParameters();

        return MetaDataInstanceFactory.createStepExecution(jobParameters);
    }

    @Test
    void 기간내_Sales가_집계되어_SalesSum이된다() throws Exception {
        long amount1 = 1000;
        long amount2 = 500;
        long amount3 = 100;

        saveSales(amount1, "1");
        saveSales(amount2, "2");
        saveSales(amount3, "3");

        reader.open(new ExecutionContext());

        assertThat(reader.read().getAmountSum()).isEqualTo(amount1 + amount2 + amount3);
        assertThat(reader.read()).isNull();
    }

    private void saveSales(long amount, String orderNo) {
        salesRepository.save(Sales.builder()
                        .orderDate(orderDate)
                        .amount(amount)
                        .orderNo(orderNo)
                .build());
    }

}
