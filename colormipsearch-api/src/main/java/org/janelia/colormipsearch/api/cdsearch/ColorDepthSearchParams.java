package org.janelia.colormipsearch.api.cdsearch;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class ColorDepthSearchParams {
    private final Map<String, Object> params = new LinkedHashMap<>();

    public String getStringParam(String name) {
        return (String) params.get(name);
    }

    public Integer getIntParam(String name, int defaultValue) {
        Object oValue = params.get(name);
        if (oValue instanceof Number) {
            return ((Number) oValue).intValue();
        } else if (oValue instanceof String) {
            String sValue = (String) oValue;
            if (StringUtils.isNotBlank(sValue)) {
                return Integer.parseInt(sValue);
            } else {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    public Long getLongParam(String name, long defaultValue) {
        Object oValue = params.get(name);
        if (oValue instanceof Number) {
            return ((Number) oValue).longValue();
        } else if (oValue instanceof String) {
            String sValue = (String) oValue;
            if (StringUtils.isNotBlank(sValue)) {
                return Long.parseLong(sValue);
            } else {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    public Double getDoubleParam(String name, double defaultValue) {
        Object oValue = params.get(name);
        if (oValue instanceof Number) {
            return ((Number) oValue).doubleValue();
        } else if (oValue instanceof String) {
            String sValue = (String) oValue;
            if (StringUtils.isNotBlank(sValue)) {
                return Double.parseDouble(sValue);
            } else {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    public Boolean getBoolParam(String name, boolean defaultValue) {
        Object oValue = params.get(name);
        if (oValue instanceof Boolean) {
            return (Boolean) oValue;
        } else if (oValue instanceof String) {
            String sValue = (String) oValue;
            return Boolean.parseBoolean(sValue);
        } else {
            return defaultValue;
        }
    }

    public ColorDepthSearchParams setParam(String name, Object value) {
        if (value != null) {
            params.put(name, value);
        }
        return this;
    }

    public Map<String, Object> asMap() {
        return params;
    }
}
