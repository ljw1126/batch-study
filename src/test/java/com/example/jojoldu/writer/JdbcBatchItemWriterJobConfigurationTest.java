package com.example.jojoldu.writer;

import com.example.jojoldu.TestBatchConfig;
import com.example.jojoldu.TestH2DataSourceConfiguration;
import com.example.jojoldu.entity.Pay;
import com.example.jojoldu.entity.PayRepository;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest
@SpringBatchTest
@ContextConfiguration(classes = {
        JdbcBatchItemWriterJobConfiguration.class,
        TestBatchConfig.class
})
@ActiveProfiles("test")
class JdbcBatchItemWriterJobConfigurationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private PayRepository payRepository;

    @AfterEach
    void tearDown() {
        payRepository.deleteAllInBatch();
    }

    @Test
    void jdbcItemWriter테스트() throws Exception {
        for(long i = 0; i < 10; i++) {
            payRepository.save(new Pay(i * 100, String.valueOf(i), LocalDateTime.now()));
        }

        JobParametersBuilder builder = new JobParametersBuilder();
        builder.addString("version", "1");

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(builder.toJobParameters());

        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(payRepository.findAll().size()).isEqualTo(20); // 같은 pay 테이블에 추가했으므로
    }
}
