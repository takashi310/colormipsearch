package org.janelia.colormipsearch.cmd.dataexport;

public interface DataExporter {
    void export(String source, long offset, int size);
}
