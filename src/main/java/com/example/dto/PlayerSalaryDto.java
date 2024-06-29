package com.example.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PlayerSalaryDto {
    private String id;
    private String lastName;
    private String firstName;
    private String position;
    private int birthYear;
    private int debutYear;
    private int salary;

    public static PlayerSalaryDto of(PlayerDto playerDto, int salary) {
        return PlayerSalaryDto.builder()
                .id(playerDto.getId())
                .lastName(playerDto.getLastName())
                .firstName(playerDto.getFirstName())
                .position(playerDto.getPosition())
                .birthYear(playerDto.getBirthYear())
                .debutYear(playerDto.getDebutYear())
                .salary(salary)
                .build();
    }
}
