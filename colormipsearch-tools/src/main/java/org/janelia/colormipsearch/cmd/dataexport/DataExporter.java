package org.janelia.colormipsearch.cmd.dataexport;

import org.janelia.colormipsearch.dataio.DataSourceParam;

public interface DataExporter {
    /**
     * Get the data source to be exported.
     * @return
     */
    DataSourceParam getDataSource();

    /**
     * Invoke data export operation.
     */
    void runExport();
}
