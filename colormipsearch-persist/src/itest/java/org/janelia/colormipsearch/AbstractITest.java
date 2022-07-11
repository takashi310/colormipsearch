package org.janelia.colormipsearch;

import java.util.Properties;

import org.junit.BeforeClass;

public class AbstractITest {
    private static final Properties ITEST_CONFIG = new Properties();

    @BeforeClass
    public static void setupITestsConfig() {
        ITEST_CONFIG.setProperty("MongoDB.ConnectionURL", "mongodb://localhost:27017");
        ITEST_CONFIG.setProperty("MongoDB.Database", "neuronbridge_test");
    }

    protected static String getTestProperty(String key, String defaultValue) {
        return ITEST_CONFIG.getProperty(key, defaultValue);
    }
}
