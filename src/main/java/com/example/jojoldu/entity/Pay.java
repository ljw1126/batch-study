package com.example.jojoldu.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class Pay {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long amount;

    @Column(name = "tx_name")
    private String txName;

    @Column(name = "tx_date_time")
    private LocalDateTime txDateTime;


    public Pay(Long amount, String txName, String txDateTime) {
        this(null, amount, txName, LocalDateTime.parse(txDateTime, FORMATTER));
    }

    public Pay(Long amount, String txName, LocalDateTime txDateTime) {
        this(null, amount, txName, txDateTime);
    }

    @Builder
    public Pay(Long id, Long amount, String txName, LocalDateTime txDateTime) {
        this.id = id;
        this.amount = amount;
        this.txName = txName;
        this.txDateTime = txDateTime;
    }
}
