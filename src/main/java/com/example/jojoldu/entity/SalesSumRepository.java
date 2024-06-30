package com.example.jojoldu.entity;

import com.example.jojoldu.entity.SalesSum;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

public interface SalesSumRepository extends JpaRepository<SalesSum, Long> {
}
