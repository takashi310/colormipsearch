package org.janelia.colormipsearch.dataio;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface CDSSessionWriter {
    Number createSession(List<DataSourceParam> masksInputs,
                         List<DataSourceParam> targetsInputs,
                         Map<String, Object> params,
                         Set<String> tags);
}
