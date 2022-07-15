package org.janelia.colormipsearch;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.junit.BeforeClass;

public class AbstractITest {
    private static final Properties ITEST_CONFIG = new Properties();

    @BeforeClass
    public static void setupITestsConfig() throws IOException {
        InputStream testConfigStream = AbstractITest.class.getClassLoader().getResourceAsStream("nbdb_itest.properties");
        ITEST_CONFIG.load(testConfigStream);
    }

    protected static String getTestProperty(String key, String defaultValue) {
        return ITEST_CONFIG.getProperty(key, defaultValue);
    }
}
