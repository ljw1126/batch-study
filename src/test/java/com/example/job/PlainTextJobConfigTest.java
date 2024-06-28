package com.example.job;

import com.example.BatchTestConfig;
import com.example.core.entity.PlainText;
import com.example.core.repository.PlainTextRepository;
import com.example.core.repository.ResultTextRepository;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.annotation.Transactional;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;


@SpringBatchTest
@SpringBootTest
@ActiveProfiles("test")
@ContextConfiguration(classes = {PlainTextJobConfig.class, BatchTestConfig.class}) // PlainTextJobConfig 대해서만 테스트, 트랜잭션 어노테이션 추가하니 remove해라 경고뜸
class PlainTextJobConfigTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private PlainTextRepository plainTextRepository;

    @Autowired
    private ResultTextRepository resultTextRepository;

    @AfterEach
    void tearDown() {
        plainTextRepository.deleteAllInBatch();
        resultTextRepository.deleteAllInBatch();
    }

    @Test
    void plainText데이터가_없을때도_JOB_이_성공한다() throws Exception {
        JobExecution execution = jobLauncherTestUtils.launchJob();

        assertThat(execution.getExitStatus()).isEqualTo(ExitStatus.COMPLETED);
        assertThat(resultTextRepository.count()).isZero();
    }

    @Test
    void plainText데이터가_있을때() throws Exception {
        givenPlainTExt(12);

        JobExecution execution = jobLauncherTestUtils.launchJob();

        assertThat(execution.getExitStatus()).isEqualTo(ExitStatus.COMPLETED);
        assertThat(resultTextRepository.count()).isEqualTo(12);
    }

    private void givenPlainTExt(int count) {
        IntStream.range(0, count)
                .forEach(num -> plainTextRepository.save(new PlainText(null, "text" + num)));
    }
}
