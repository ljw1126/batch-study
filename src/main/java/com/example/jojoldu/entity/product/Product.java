package com.example.jojoldu.entity.product;

import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.LocalDate;

@Getter
@Entity
@Table
@NoArgsConstructor
public class Product {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;
    private Long price;
    private LocalDate createDate;

    @Enumerated(EnumType.STRING)
    private ProductStatus status;

    public Product(Long price, LocalDate createDate) {
        this(String.valueOf(price), price, createDate, ProductStatus.APPROVE);
    }

    public Product(String name, Long price, LocalDate createDate, ProductStatus status) {
        this.name = name;
        this.price = price;
        this.createDate = createDate;
        this.status = status;
    }
}
