package dev.nishisan.utils.stats.dto;

public class SimpleValueDTO {
    private String name;
    private Long value;

    public SimpleValueDTO(String name, Long value) {
        this.name = name;
        this.value = value;
    }

    public SimpleValueDTO(String name)   {
        this.name = name;
        this.value = 0L;
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
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }
}
