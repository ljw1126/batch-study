package com.example.job.player;

import com.example.dto.PlayerDto;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class PlayerFieldSetMapper implements FieldSetMapper<PlayerDto> {
    @Override
    public PlayerDto mapFieldSet(FieldSet fieldSet) throws BindException {
        return PlayerDto.builder()
                .id(fieldSet.readString(0))
                .lastName(fieldSet.readString(1))
                .firstName(fieldSet.readString(2))
                .position(fieldSet.readString(3))
                .birthYear(fieldSet.readInt(4))
                .debutYear(fieldSet.readInt(5))
                .build();
    }
}
