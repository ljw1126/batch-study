package com.example;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@SpringBootApplication
public class BacthStudyApplication {

	public static void main(String[] args) {
		SpringApplication.run(BacthStudyApplication.class, args);
	}

}
