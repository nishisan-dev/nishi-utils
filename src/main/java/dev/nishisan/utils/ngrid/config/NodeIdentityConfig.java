package dev.nishisan.utils.ngrid.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeIdentityConfig {

    private String id;
    private String host;
    private int port;
    private Set<String> roles = Collections.emptySet();
    private DirsConfig dirs;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Set<String> getRoles() {
        return roles;
    }

    public void setRoles(Set<String> roles) {
        this.roles = roles;
    }

    public DirsConfig getDirs() {
        return dirs;
    }

    public void setDirs(DirsConfig dirs) {
        this.dirs = dirs;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DirsConfig {
        @JsonProperty("base")
        private String base;

        public String getBase() {
            return base;
        }

        public void setBase(String base) {
            this.base = base;
        }
    }
}
