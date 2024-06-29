package com.example.core;

import com.example.dto.PlayerDto;
import com.example.dto.PlayerSalaryDto;
import org.springframework.stereotype.Service;

import java.time.Year;

@Service
public class PlayerSalaryService {

    public PlayerSalaryDto calcSalary(PlayerDto playerDto) {
        int base = 1_000_000;
        int salary = (Year.now().getValue() - playerDto.getBirthYear()) * base;
        return PlayerSalaryDto.of(playerDto, salary);
    }
}
