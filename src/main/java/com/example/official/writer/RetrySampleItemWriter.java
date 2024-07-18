package com.example.official.writer;

import org.springframework.batch.core.step.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

public class RetrySampleItemWriter<T> implements ItemWriter<T> {

    private int counter = 0;

    @Override
    public void write(List<? extends T> items) throws Exception {
        int current = counter;
        counter += items.size();
        if (current < 3 && (counter >= 2 || counter >= 3)) {
            throw new IllegalStateException("Temporary error");
        }
    }

    /**
     * @return number of times {@link #write(List)} method was called.
     */
    public int getCounter() {
        return counter;
    }

}
