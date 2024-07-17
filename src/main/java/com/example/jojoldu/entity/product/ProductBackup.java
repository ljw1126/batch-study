package com.example.jojoldu.entity.product;

import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.LocalDate;

@Getter
@Entity
@Table
@NoArgsConstructor
public class ProductBackup {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long originId;

    private String name;
    private Long price;
    private LocalDate createDate;

    public ProductBackup(Product product) {
        this(product.getId(), product.getName(), product.getPrice(), product.getCreateDate());
    }

    public ProductBackup(Long originId, String name, Long price, LocalDate createDate) {
        this.originId = originId;
        this.name = name;
        this.price = price;
        this.createDate = createDate;
    }
}
