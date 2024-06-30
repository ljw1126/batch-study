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

@Entity
@Getter
@Setter // BeanPropertyRowMapper 지정시 필요
@NoArgsConstructor
public class SalesSum {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_date")
    private LocalDate orderDate;

    @Column(name = "amount_sum")
    private long amountSum;


    public SalesSum(LocalDate orderDate, long amountSum) {
        this(null, orderDate, amountSum);
    }

    @Builder
    public SalesSum(Long id, LocalDate orderDate, long amountSum) {
        this.id = id;
        this.orderDate = orderDate;
        this.amountSum = amountSum;
    }
}
