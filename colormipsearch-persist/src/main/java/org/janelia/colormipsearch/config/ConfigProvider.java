package org.janelia.colormipsearch.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigProvider.class);

    private ConfigImpl config = new ConfigImpl();

    public ConfigProvider fromResource(String resourceName) {
        if (StringUtils.isBlank(resourceName)) {
            return this;
        }
        try (InputStream configStream = this.getClass().getClassLoader().getResourceAsStream(resourceName)) {
            return fromInputStream(configStream);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public ConfigProvider fromFile(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return this;
        }
        File file = new File(fileName);
        if (file.exists() && file.isFile()) {
            try (InputStream fileInputStream = new FileInputStream(file)) {
                LOG.info("Reading application config from file {}", file);
                return fromInputStream(fileInputStream);
            } catch (IOException e) {
                LOG.error("Error reading configuration file {}", fileName, e);
                throw new UncheckedIOException(e);
            }
        } else {
            LOG.warn("Configuration file {} not found", fileName);
        }
        return this;

    }

    public ConfigProvider fromMap(Map<String, String> map) {
        config.properties.putAll(map);
        return this;
    }

    private ConfigProvider fromInputStream(InputStream stream) {
        try {
            config.properties.load(stream);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return this;
    }

    public Config get() {
        return config;
    }
}
