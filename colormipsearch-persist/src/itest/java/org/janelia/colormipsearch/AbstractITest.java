package org.janelia.colormipsearch;

import org.janelia.colormipsearch.config.Config;
import org.janelia.colormipsearch.config.ConfigProvider;
import org.junit.BeforeClass;

public class AbstractITest {
    private static Config itestConfig;

    @BeforeClass
    public static void setupITestsConfig() {
        itestConfig = ConfigProvider.getInstance()
                .fromResource("./nbdb_itest.properties")
                .get();
    }

    protected static Config getITestConfig() {
        return itestConfig;
    }
}
