package org.janelia.colormipsearch.model;

public class FileData {
    enum FileDataType {
        file,
        zipEntry
    };

    private FileDataType dataType;
    private String fileName;
    private String entryName;

    public FileDataType getDataType() {
        return dataType;
    }

    public void setDataType(FileDataType dataType) {
        this.dataType = dataType;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getEntryName() {
        return entryName;
    }

    public void setEntryName(String entryName) {
        this.entryName = entryName;
    }
}
