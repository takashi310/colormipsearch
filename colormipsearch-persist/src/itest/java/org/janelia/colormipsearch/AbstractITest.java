package org.janelia.colormipsearch;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.config.Config;
import org.janelia.colormipsearch.config.ConfigProvider;
import org.janelia.colormipsearch.dao.DaosProvider;
import org.junit.BeforeClass;

public class AbstractITest {
    private static Config itestConfig;

    @BeforeClass
    public static void setupITestsConfig() {
        itestConfig = new ConfigProvider()
                .fromResource("./nbdb_itest.properties")
                .get();
    }

    protected static Config getITestConfig() {
        return itestConfig;
    }
}
