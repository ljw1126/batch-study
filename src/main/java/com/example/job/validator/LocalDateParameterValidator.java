package com.example.job.validator;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.util.StringUtils;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;

public class LocalDateParameterValidator implements JobParametersValidator {

    private final String parameterName;

    public LocalDateParameterValidator(String parameterName) {
        this.parameterName = parameterName;
    }

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        String localDate = parameters.getString(parameterName);

        if(!StringUtils.hasText(localDate)) {
            throw new JobParametersInvalidException(parameterName + "가 빈 문자열이거나 존재하지 않습니다");
        }

        try {
            LocalDate.parse(localDate);
        } catch (DateTimeParseException ex) {
            throw new JobParametersInvalidException(parameterName + "가 날짜 형식의 문자열이 아닙니다");
        }
    }
}
