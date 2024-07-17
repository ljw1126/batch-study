package com.example.jojoldu.parallel;

import com.example.jojoldu.TestBatchConfig;
import com.example.jojoldu.entity.product.Product;
import com.example.jojoldu.entity.product.ProductBackup;
import com.example.jojoldu.entity.product.ProductBackupRepository;
import com.example.jojoldu.entity.product.ProductRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.time.format.DateTimeFormatter.ofPattern;
import static org.assertj.core.api.Assertions.assertThat;


/**
 * 출처
 * https://jojoldu.tistory.com/550
 *
 * master step에서 파티셔닝을 한 후 slave step이 병렬로 실행됨을 확인
 */
@SpringBatchTest
@SpringBootTest
@ContextConfiguration(classes = {PartitionLocalConfiguration.class, TestBatchConfig.class})
@ActiveProfiles("test")
class PartitionLocalConfigurationTest {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionLocalConfigurationTest.class);
    public static final DateTimeFormatter FORMATTER = ofPattern("yyyy-MM-dd");

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private ProductBackupRepository productBackupRepository;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @AfterEach
    void tearDown() {
        productBackupRepository.deleteAllInBatch();
        productRepository.deleteAllInBatch();
    }

    @Test
    void Product가_ProductBackup으로_이관된다() throws Exception {
        //given
        LocalDate txDate = LocalDate.of(2024, 7, 17);

        List<Product> products = new ArrayList<>();
        int expectedCount = 50;
        for(int i = 1; i <= expectedCount; i++) {
            products.add(new Product((long) i, txDate));
        }
        productRepository.saveAll(products);

        JobParameters jobParameters = jobLauncherTestUtils.getUniqueJobParametersBuilder()
                .addString("startDate", txDate.format(FORMATTER))
                .addString("endDate", txDate.plusDays(1).format(FORMATTER))
                .toJobParameters();

        // when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        // then
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        List<ProductBackup> backups = productBackupRepository.findAll();
        assertThat(backups.size()).isEqualTo(expectedCount);

        List<Map<String, Object>> metaTable = jdbcTemplate.queryForList("select step_name, status, commit_count, read_count, write_count from BATCH_STEP_EXECUTION");

        for (Map<String, Object> step : metaTable) {
            LOG.info("meta table row={}", step);
        }
    }
}
