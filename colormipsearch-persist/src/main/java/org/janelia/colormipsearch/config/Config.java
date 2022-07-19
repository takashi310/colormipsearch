package org.janelia.colormipsearch.config;

public interface Config {
    String getStringPropertyValue(String name);
    String getStringPropertyValue(String name, String defaultValue);
    Boolean getBooleanPropertyValue(String name);
    Boolean getBooleanPropertyValue(String name, Boolean defaultValue);
    Double getDoublePropertyValue(String name);
    Double getDoublePropertyValue(String name, Double defaultValue);
    Integer getIntegerPropertyValue(String name);
    Integer getIntegerPropertyValue(String name, Integer defaultValue);
    Long getLongPropertyValue(String name);
    Long getLongPropertyValue(String name, Long defaultValue);
}
