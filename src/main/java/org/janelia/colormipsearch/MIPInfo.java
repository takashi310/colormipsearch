package org.janelia.colormipsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

class MIPInfo implements Serializable {
    @JsonProperty
    String id;
    @JsonProperty
    String type = "file"; // this can be file or zip
    @JsonProperty
    String libraryName;
    @JsonProperty
    String publishedName;
    @JsonProperty
    String imagePath;
    @JsonProperty
    String cdmPath;
    @JsonProperty
    String archivePath;
    @JsonProperty
    String imageURL;
    @JsonProperty
    String thumbnailURL;
    @JsonProperty
    String relatedImageRefId;
    @JsonProperty("attrs")
    Map<String, String> attrs = new LinkedHashMap<>();

    MIPInfo() {
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

    boolean isEmSkelotonMIP() {
        return libraryName != null && StringUtils.equalsIgnoreCase(libraryName, "flyem_hemibrain") ||
                cdmPath != null && (StringUtils.containsIgnoreCase(cdmPath, "flyem") || StringUtils.containsIgnoreCase(cdmPath, "hemi"));
    }

    boolean exists() {
        if (StringUtils.equalsIgnoreCase("zipEntry", type)) {
            ZipFile archiveFile;
            try {
                archiveFile = new ZipFile(archivePath);
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
        } else {
            return new File(imagePath).exists();
        }
    }

    InputStream openInputStream() throws IOException {
        if (StringUtils.equalsIgnoreCase("zipEntry", type)) {
            ZipFile archiveFile = new ZipFile(archivePath);
            try {
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
            } finally {
            }
        } else {
            return new FileInputStream(imagePath);
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
