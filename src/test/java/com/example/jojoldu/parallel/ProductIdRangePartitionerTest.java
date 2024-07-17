package com.example.jojoldu.parallel;

import com.example.jojoldu.entity.product.ProductRepository;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.item.ExecutionContext;

import java.time.LocalDate;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProductIdRangePartitionerTest {

    @Mock
    private ProductRepository productRepository;

    @Test
    void gridSize에_맞게_id가_분할된다() {
        lenient()
                .when(productRepository.findMinId(any(LocalDate.class), any(LocalDate.class)))
                .thenReturn(1L);

        lenient()
                .when(productRepository.findMaxId(any(LocalDate.class), any(LocalDate.class)))
                .thenReturn(10L);

        ProductIdRangePartitioner partitioner
                = new ProductIdRangePartitioner(productRepository, LocalDate.of(2024, 7, 17), LocalDate.of(2024, 7, 24));

        Map<String, ExecutionContext> executionContextMap = partitioner.partition(5); // 1 ~ 10의 레코드를 5개로 StepExcution 생성

        ExecutionContext executionContext0 = executionContextMap.get("partition0");
        assertThat(executionContext0.getLong("minId")).isEqualTo(1L);
        assertThat(executionContext0.getLong("maxId")).isEqualTo(2L);

        ExecutionContext executionContext4 = executionContextMap.get("partition4");
        assertThat(executionContext4.getLong("minId")).isEqualTo(9L);
        assertThat(executionContext4.getLong("maxId")).isEqualTo(10L);

    }

}
