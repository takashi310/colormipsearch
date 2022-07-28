package org.janelia.colormipsearch.dataio;

import java.util.List;
import java.util.Map;

public interface CDSSessionWriter {
    Number createSession(List<DataSourceParam> masksInputs,
                         List<DataSourceParam> targetsInputs,
                         Map<String, Object> params);
}
