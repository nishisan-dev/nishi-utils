package dev.nishisan.utils.ngrid.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Configuration for cluster autodiscovery.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AutodiscoverConfig {
    private boolean enabled;
    private String secret;
    private String seed;

    /** Creates a default autodiscover config. */
    public AutodiscoverConfig() {
    }

    /**
     * Returns whether autodiscovery is enabled.
     *
     * @return {@code true} if autodiscovery is enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables or disables autodiscovery.
     *
     * @param enabled {@code true} to enable autodiscovery
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Returns the shared secret used for autodiscovery authentication.
     *
     * @return the shared secret
     */
    public String getSecret() {
        return secret;
    }

    /**
     * Sets the shared secret used for autodiscovery authentication.
     *
     * @param secret the shared secret
     */
    public void setSecret(String secret) {
        this.secret = secret;
    }

    /**
     * Returns the seed address used to bootstrap peer discovery.
     *
     * @return the seed address
     */
    public String getSeed() {
        return seed;
    }

    /**
     * Sets the seed address used to bootstrap peer discovery.
     *
     * @param seed the seed address
     */
    public void setSeed(String seed) {
        this.seed = seed;
    }
}
