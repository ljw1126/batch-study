package com.example.core.entity;

import lombok.Getter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;


@Getter
@Entity
@Table(name = "result_text")
public class ResultText {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String text;

    public ResultText() {
    }

    public ResultText(Long id, String text) {
        this.id = id;
        this.text = text;
    }
}
