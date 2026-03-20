package dev.nishisan.utils.ngrid.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serial;
import java.io.Serializable;

/**
 * Configuration for a distributed map policy, deserialized from YAML/JSON.
 */
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

    /** Creates a default map policy config. */
    public MapPolicyConfig() {
    }

    /**
     * Returns the map name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the map name.
     *
     * @param name the name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the key type class name.
     *
     * @return the key type
     */
    public String getKeyType() {
        return keyType;
    }

    /**
     * Sets the key type class name.
     *
     * @param keyType the key type
     */
    public void setKeyType(String keyType) {
        this.keyType = keyType;
    }

    /**
     * Returns the value type class name.
     *
     * @return the value type
     */
    public String getValueType() {
        return valueType;
    }

    /**
     * Sets the value type class name.
     *
     * @param valueType the value type
     */
    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    /**
     * Returns the persistence mode.
     *
     * @return the persistence mode
     */
    public String getPersistence() {
        return persistence;
    }

    /**
     * Sets the persistence mode.
     *
     * @param persistence the persistence mode
     */
    public void setPersistence(String persistence) {
        this.persistence = persistence;
    }
}
