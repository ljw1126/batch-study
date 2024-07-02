package com.example.jojoldu.writer;

import com.example.jojoldu.TestBatchConfig;
import com.example.jojoldu.entity.StudentRepository;
import com.example.jojoldu.entity.TeacherRepository;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest
@SpringBatchTest
@ContextConfiguration(classes = {
        JpaItemWriterJobConfiguration.class,
        TestBatchConfig.class
})
@ActiveProfiles("test")
class JpaItemWriterJobConfigurationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private TeacherRepository teacherRepository;

    @Autowired
    private StudentRepository studentRepository;

    @AfterEach
    void tearDown() {
        studentRepository.deleteAllInBatch();
        teacherRepository.deleteAllInBatch();
    }

    @Test
    void jpaItemWriter테스트() throws Exception {
        JobParameters jobParameters = jobLauncherTestUtils.getUniqueJobParametersBuilder()
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(teacherRepository.count()).isEqualTo(2);
        assertThat(studentRepository.count()).isEqualTo(2);
    }
}
