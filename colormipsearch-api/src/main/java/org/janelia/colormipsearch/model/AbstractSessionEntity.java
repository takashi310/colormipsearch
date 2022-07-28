package org.janelia.colormipsearch.model;

import java.util.LinkedHashMap;
import java.util.Map;

import org.janelia.colormipsearch.model.annotations.PersistenceInfo;

/**
 * This is the representation of a color depth or PPP match run.
 */
@PersistenceInfo(storeName ="matchSession")
public abstract class AbstractSessionEntity extends AbstractBaseEntity {
    // this is the username of the person who ran the process
    private String username;
    private Map<String, Object> params = new LinkedHashMap<>();

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    public void putParams(Map<String, Object> params) {
        this.params.putAll(params);
    }
}
