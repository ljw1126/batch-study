package com.example.jojoldu.writer;

import com.example.jojoldu.entity.Student;
import com.example.jojoldu.entity.Teacher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;
import java.util.List;


@Slf4j
@Configuration
public class JpaItemWriterJobConfiguration {

    public static final String JOB_NAME = "jpaItemWriterJob";

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;

    public JpaItemWriterJobConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, EntityManagerFactory entityManagerFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.entityManagerFactory = entityManagerFactory;
    }

    private int chunkSize;

    @Value("${chunkSize:10}")
    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Bean(name = JOB_NAME)
    public Job job() {
        return jobBuilderFactory.get(JOB_NAME)
                .start(step())
                .build();
    }

    @Bean(name = JOB_NAME + "Step")
    @JobScope
    public Step step() {
        return stepBuilderFactory.get(JOB_NAME + "Step")
                .<Teacher, Teacher>chunk(chunkSize)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }

    @Bean(name = JOB_NAME + "Reader")
    @StepScope
    public ListItemReader<Teacher> reader() {
        return new ListItemReader<>(List.of(new Teacher("수학선생님", "수학"), new Teacher("영어선생님", "영어")));
    }

    @Bean(name = JOB_NAME + "Processor")
    @StepScope
    public ItemProcessor<Teacher, Teacher> processor() {
        return teacher -> {
            teacher.addStudent(new Student("신규 제자"));
            return teacher;
        };
    }

    @Bean(name = JOB_NAME + "Writer")
    @StepScope
    public JpaItemWriter<Teacher> writer() {
        return new JpaItemWriterBuilder<Teacher>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }

}
