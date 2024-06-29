package com.example.job.parallel;

import com.example.dto.AmoutDto;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class AmountFieldSetMapper implements FieldSetMapper<AmoutDto> {
    @Override
    public AmoutDto mapFieldSet(FieldSet fieldSet) throws BindException {
        return AmoutDto.builder()
                .index(fieldSet.readInt(0))
                .name(fieldSet.readString(1))
                .amount(fieldSet.readInt(2))
                .build();
    }
}
