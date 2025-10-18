package dev.nishisan.utils.stats.dto;

import java.util.concurrent.atomic.AtomicLong;

public class SimpleValueDTO {
    private String name;
    private AtomicLong value;

    public SimpleValueDTO(String name, Long value) {
        this.name = name;
        this.value = new AtomicLong(value);
    }

    public SimpleValueDTO(String name)   {
        this.name = name;
        this.value = new AtomicLong(0L);
    }

    public SimpleValueDTO()   {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getValue() {
        return this.value.get();
    }

    public void setValue(Long value) {
        this.value.set(value);
    }
}
