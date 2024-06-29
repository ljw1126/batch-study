package com.example.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class AmoutDto {
    private int index;
    private String name;
    private int amount;
}
