package org.janelia.colormipsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Collections;
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

    MIPInfo() {
    }

    MIPInfo(MIPInfo that) {
        this.id = that.id;
        this.libraryName = that.libraryName;
        this.publishedName = that.publishedName;
        this.imagePath = that.imagePath;
        this.cdmPath = that.cdmPath;
        this.imageURL = that.imageURL;
        this.thumbnailURL = that.thumbnailURL;
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
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(cdmPath)
                .append(imagePath)
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
                return archiveFile.getEntry(imagePath) != null;
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
                    try {
                        archiveFile.close();
                    } catch (IOException ignore) {
                    }
                    return null;
                }
            } finally {
            }
        } else {
            return new FileInputStream(imagePath);
        }
    }
}
