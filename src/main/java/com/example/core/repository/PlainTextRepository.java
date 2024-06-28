package com.example.core.repository;

import com.example.core.entity.PlainText;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PlainTextRepository extends JpaRepository<PlainText, Long> {
    Page<PlainText> findBy(Pageable pageable);
}
