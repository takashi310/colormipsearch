package org.janelia.colormipsearch.dataio;

import java.util.List;
import java.util.Map;

public interface CDSParamsWriter {
    void writeParams(List<DataSourceParam> masksInputs,
                     List<DataSourceParam> targetsInputs,
                     Map<String, Object> params);
}
