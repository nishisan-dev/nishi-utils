package dev.nishisan.utils.ngrid.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serial;
import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MapPolicyConfig implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private String name;
    @JsonProperty("key-type")
    private String keyType;
    @JsonProperty("value-type")
    private String valueType;
    private String persistence; // DISABLED, ASYNC_NO_FSYNC, ASYNC_WITH_FSYNC

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getKeyType() {
        return keyType;
    }

    public void setKeyType(String keyType) {
        this.keyType = keyType;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    public String getPersistence() {
        return persistence;
    }

    public void setPersistence(String persistence) {
        this.persistence = persistence;
    }
}
