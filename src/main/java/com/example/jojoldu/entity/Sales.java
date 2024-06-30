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
import java.time.LocalDate;

@Getter
@Setter
@NoArgsConstructor
@Entity
public class Sales {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_date")
    private LocalDate orderDate;

    @Column
    private long amount;

    @Column(name = "order_no")
    private String orderNo;

    @Builder
    public Sales(Long id, LocalDate orderDate, long amount, String orderNo) {
        this.id = id;
        this.orderDate = orderDate;
        this.amount = amount;
        this.orderNo = orderNo;
    }
}
