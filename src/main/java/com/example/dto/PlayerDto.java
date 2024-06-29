package com.example.dto;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class PlayerDto {
    private String id;
    private String lastName;
    private String firstName;
    private String position;
    private int birthYear;
    private int debutYear;
}
