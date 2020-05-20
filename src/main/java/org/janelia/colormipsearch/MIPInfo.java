package org.janelia.colormipsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class MIPInfo implements Serializable {
    private String id;
    private String type = "file"; // this can be file or zip
    private String libraryName;
    private String publishedName;
    private String imagePath;
    private String cdmPath;
    private String archivePath;
    private String imageURL;
    private String thumbnailURL;
    private String relatedImageRefId;
    @JsonProperty("attrs")
    private Map<String, String> attrs = new LinkedHashMap<>();

    public MIPInfo() {
    }

    @JsonProperty
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @JsonProperty
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty
    public String getLibraryName() {
        return libraryName;
    }

    public void setLibraryName(String libraryName) {
        this.libraryName = libraryName;
    }

    @JsonProperty
    public String getPublishedName() {
        return publishedName;
    }

    public void setPublishedName(String publishedName) {
        this.publishedName = publishedName;
    }

    @JsonProperty
    public String getImagePath() {
        return imagePath;
    }

    public void setImagePath(String imagePath) {
        this.imagePath = imagePath;
    }

    @JsonProperty
    public String getCdmPath() {
        return cdmPath;
    }

    public void setCdmPath(String cdmPath) {
        this.cdmPath = cdmPath;
    }

    @JsonProperty
    public String getArchivePath() {
        return archivePath;
    }

    public void setArchivePath(String archivePath) {
        this.archivePath = archivePath;
    }

    @JsonProperty
    public String getImageURL() {
        return imageURL;
    }

    public void setImageURL(String imageURL) {
        this.imageURL = imageURL;
    }

    @JsonProperty
    public String getThumbnailURL() {
        return thumbnailURL;
    }

    public void setThumbnailURL(String thumbnailURL) {
        this.thumbnailURL = thumbnailURL;
    }

    @JsonProperty
    public String getRelatedImageRefId() {
        return relatedImageRefId;
    }

    public void setRelatedImageRefId(String relatedImageRefId) {
        this.relatedImageRefId = relatedImageRefId;
    }

    public void addAttr(String name, String value) {
        attrs.put(name, value);
    }

    public void iterateAttrs(BiConsumer<String, String> attrConsumer) {
        this.attrs.forEach(attrConsumer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        MIPInfo mipImage = (MIPInfo) o;

        return new EqualsBuilder()
                .append(id, mipImage.id)
                .append(cdmPath, mipImage.cdmPath)
                .append(imagePath, mipImage.imagePath)
                .append(archivePath, mipImage.archivePath)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(cdmPath)
                .append(imagePath)
                .append(archivePath)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("cdmPath", cdmPath)
                .append("imagePath", imagePath)
                .toString();
    }

    public boolean exists() {
        if (StringUtils.equalsIgnoreCase("zipEntry", type)) {
            Path archiveFilePath = Paths.get(archivePath);
            if (Files.isDirectory(archiveFilePath)) {
                return checkFSDir(archiveFilePath);
            } else if (Files.isRegularFile(archiveFilePath)) {
                return checkZipEntry(archiveFilePath);
            } else {
                return false;
            }
        } else {
            return new File(imagePath).exists();
        }
    }

    private boolean checkFSDir(Path archiveFilePath) {
        return Files.exists(archiveFilePath.resolve(imagePath));
    }

    private boolean checkZipEntry(Path archiveFilePath) {
        ZipFile archiveFile;
        try {
            archiveFile = new ZipFile(archiveFilePath.toFile());
        } catch (IOException e) {
            return false;
        }
        try {
            if (archiveFile.getEntry(imagePath) != null) {
                return true;
            } else {
                // slightly longer test
                String imageFn = Paths.get(imagePath).getFileName().toString();
                return archiveFile.stream()
                        .filter(ze -> !ze.isDirectory())
                        .map(ze -> Paths.get(ze.getName()).getFileName().toString())
                        .filter(fn -> imageFn.equals(fn))
                        .findFirst()
                        .map(fn -> true)
                        .orElse(false);
            }
        } finally {
            try {
                archiveFile.close();
            } catch (IOException ignore) {
            }
        }
    }

    InputStream openInputStream() throws IOException {
        if (StringUtils.equalsIgnoreCase("zipEntry", type)) {
            Path archiveFilePath = Paths.get(archivePath);
            if (Files.isDirectory(archiveFilePath)) {
                return openFileStream(archiveFilePath);
            } else if (Files.isRegularFile(archiveFilePath)) {
                return openZipEntryStream(archiveFilePath);
            } else {
                return null;
            }
        } else {
            return new FileInputStream(imagePath);
        }
    }

    private InputStream openFileStream(Path archiveFilePath) throws IOException {
        return Files.newInputStream(archiveFilePath.resolve(imagePath));
    }

    private InputStream openZipEntryStream(Path archiveFilePath) throws IOException {
        ZipFile archiveFile = new ZipFile(archivePath);
        ZipEntry ze = archiveFile.getEntry(imagePath);
        if (ze != null) {
            return archiveFile.getInputStream(ze);
        } else {
            String imageFn = Paths.get(imagePath).getFileName().toString();
            return archiveFile.stream()
                    .filter(aze -> !aze.isDirectory())
                    .filter(aze -> imageFn.equals(Paths.get(aze.getName()).getFileName().toString()))
                    .findFirst()
                    .map(aze -> getEntryStream(archiveFile, aze))
                    .orElseGet(() -> {
                        try {
                            archiveFile.close();
                        } catch (IOException ignore) {
                        }
                        return null;
                    });
        }
    }

    private InputStream getEntryStream(ZipFile archiveFile, ZipEntry zipEntry) {
        try {
            return archiveFile.getInputStream(zipEntry);
        } catch (IOException e) {
            return null;
        }
    }
}
