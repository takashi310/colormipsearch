package org.janelia.colormipsearch.config;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

public class ConfigImpl implements Config {

    Properties properties = new Properties();

    @Override
    public String getStringPropertyValue(String name) {
        return properties.getProperty(name);
    }

    @Override
    public String getStringPropertyValue(String name, String defaultValue) {
        return properties.getProperty(name, defaultValue);
    }

    @Override
    public Boolean getBooleanPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isNotBlank(stringValue) ? Boolean.valueOf(stringValue) : Boolean.FALSE;
    }

    @Override
    public Boolean getBooleanPropertyValue(String name, Boolean defaultValue) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isNotBlank(stringValue) ? Boolean.valueOf(stringValue) : defaultValue;
    }

    @Override
    public Double getDoublePropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isNotBlank(stringValue) ? Double.valueOf(stringValue) : null;
    }

    @Override
    public Double getDoublePropertyValue(String name, Double defaultValue) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isNotBlank(stringValue) ? Double.valueOf(stringValue) : defaultValue;
    }

    @Override
    public Integer getIntegerPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isNotBlank(stringValue) ? Integer.valueOf(stringValue) : null;
    }

    @Override
    public Integer getIntegerPropertyValue(String name, Integer defaultValue) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isNotBlank(stringValue) ? Integer.valueOf(stringValue) : defaultValue;
    }

    @Override
    public Long getLongPropertyValue(String name) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isNotBlank(stringValue) ? Long.valueOf(stringValue) : null;
    }

    @Override
    public Long getLongPropertyValue(String name, Long defaultValue) {
        String stringValue = getStringPropertyValue(name);
        return StringUtils.isNotBlank(stringValue) ? Long.valueOf(stringValue) : defaultValue;
    }
}
