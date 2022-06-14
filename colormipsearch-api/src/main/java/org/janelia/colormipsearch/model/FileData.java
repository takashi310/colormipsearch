package org.janelia.colormipsearch.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.commons.lang3.StringUtils;

public class FileData {
    public enum FileDataType {
        file,
        zipEntry
    };

    public static FileData fromFile(String fn) {
        FileData fd = new FileData();
        fd.setDataType(FileDataType.file);
        fd.setFileName(fn);
        return fd;
    }
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

    @JsonIgnore
    public String getName() {
        return StringUtils.isNotBlank(entryName) ? entryName : fileName;
    }
}
