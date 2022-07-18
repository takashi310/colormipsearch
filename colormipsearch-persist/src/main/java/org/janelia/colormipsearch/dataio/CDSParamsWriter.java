package org.janelia.colormipsearch.dataio;

import java.util.List;
import java.util.Map;

public interface CDSParamsWriter {
    void writeParams(List<InputParam> masksInputs,
                     List<InputParam> targetsInputs,
                     Map<String, Object> params);
}
