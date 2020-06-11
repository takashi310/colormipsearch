package org.janelia.colormipsearch.tools;

import java.util.Map;
import java.util.function.Consumer;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class AbstractMetadata {
    private String id;
    private String publishedName;
    private String libraryName;
    private String cdmPath;
    private String imageName;
    private String imageArchivePath;
    private String imageType;
    private String imageURL;
    private String thumbnailURL;
    private String slideCode;
    private String objective;
    private String gender;
    private String anatomicalArea;
    private String alignmentSpace;
    private String channel;
    private String mountingProtocol;
    private Boolean publishedToStaging;

    @JsonProperty
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @JsonProperty
    public String getPublishedName() {
        return publishedName;
    }

    public void setPublishedName(String publishedName) {
        this.publishedName = publishedName;
    }

    @JsonIgnore
    public Boolean getPublishedToStaging() {
        return publishedToStaging;
    }

    public void setPublishedToStaging(Boolean publishedToStaging) {
        this.publishedToStaging = publishedToStaging;
    }

    public String getCdmPath() {
        return cdmPath;
    }

    public void setCdmPath(String cdmPath) {
        this.cdmPath = cdmPath;
    }

    public String getImageName() {
        return imageName;
    }

    public void setImageName(String imageName) {
        this.imageName = imageName;
    }

    public String getImageArchivePath() {
        return imageArchivePath;
    }

    public void setImageArchivePath(String imageArchivePath) {
        this.imageArchivePath = imageArchivePath;
    }

    public String getImageType() {
        return imageType;
    }

    public void setImageType(String imageType) {
        this.imageType = imageType;
    }

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
    public String getLibraryName() {
        return libraryName;
    }

    public void setLibraryName(String libraryName) {
        this.libraryName = libraryName;
    }

    public String getSlideCode() {
        return slideCode;
    }

    public void setSlideCode(String slideCode) {
        this.slideCode = slideCode;
    }

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getAnatomicalArea() {
        return anatomicalArea;
    }

    public void setAnatomicalArea(String anatomicalArea) {
        this.anatomicalArea = anatomicalArea;
    }

    public String getAlignmentSpace() {
        return alignmentSpace;
    }

    public void setAlignmentSpace(String alignmentSpace) {
        this.alignmentSpace = alignmentSpace;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getMountingProtocol() {
        return mountingProtocol;
    }

    public void setMountingProtocol(String mountingProtocol) {
        this.mountingProtocol = mountingProtocol;
    }

    /**
     * This method is practically used for migrating the data from the former structure
     * that had the "attrs" map to the new one where all the attributes were moved to the top level.
     *
     * @param attribute
     * @param value
     */
    @JsonAnySetter
    private void addAttr(String attribute, String value) {
        attributeValueHandler(mapAttr(attribute)).accept(value);
    }

    /**
     * This method is used for data migration in order to support both the "legacy" structure
     * and the new one that does not use the attrs map.
     *
     * @param attrs
     */
    @JsonProperty
    private void setAttrs(Map<String, String> attrs) {
        if (attrs != null) {
            attrs.forEach((k, v) -> addAttr(k, v));
        }
    }

    public void copyTo(AbstractMetadata that) {
        that.id = this.id;
        that.publishedName = this.publishedName;
        that.libraryName = this.libraryName;
        that.imageURL = this.imageURL;
        that.thumbnailURL = this.thumbnailURL;
        // copy the "attributes"
        that.slideCode = this.slideCode;
        that.objective = this.objective;
        that.gender = this.gender;
        that.anatomicalArea = this.anatomicalArea;
        that.alignmentSpace = this.alignmentSpace;
        that.channel = this.channel;
        that.mountingProtocol = this.mountingProtocol;
        that.publishedToStaging = this.publishedToStaging;
    }

    Consumer<String> attributeValueHandler(String attrName) {
        if (StringUtils.isBlank(attrName)) {
            return (attrValue) -> {}; // do nothing handler
        } else {
            switch(attrName) {
                case "publishedName":
                    return this::setPublishedName;
                case "library":
                    return this::setLibraryName;
                case "slideCode":
                    return this::setSlideCode;
                case "gender":
                    return this::setGender;
                case "anatomicalArea":
                    return this::setAnatomicalArea;
                case "alignmentSpace":
                    return this::setAlignmentSpace;
                case "objective":
                    return this::setObjective;
                case "channel":
                    return this::setChannel;
                case "mountingProtocol":
                    return this::setMountingProtocol;
                case "imageType":
                    return this::setImageType;
                case "imageName":
                    return this::setImageName;
                case "imageURL":
                    return this::setImageURL;
                case "thumbnailURL":
                    return this::setThumbnailURL;
                default:
                    return defaultAttributeValueHandler(attrName);
            }
        }
    }

    Consumer<String> defaultAttributeValueHandler(String attrName) {
        return (attrValue) -> {}; // do nothing
    }

    String mapAttr(String attrName) {
        if (StringUtils.isBlank(attrName)) {
            return null;
        }
        switch(attrName.toLowerCase()) {
            case "published name":
            case "publishedname":
                return "publishedName";
            case "library":
                return "library";
            case "slide code":
                return "slideCode";
            case "gender":
                return "gender";
            case "anatomical area":
                return "anatomicalArea";
            case "alignment space":
                return "alignmentSpace";
            case "objective":
                return "objective";
            case "channel":
                return "channel";
            case "mounting protocol":
                return "mountingProtocol";
            case "type":
            case "imagetype":
                return "imageType";
            case "imagepath":
            case "imagename":
                return "imageName";
            case "image_path":
                return "imageURL";
            case "thumbnail_path":
                return "thumbnailURL";
        }
        return attrName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        AbstractMetadata mipMetadata = (AbstractMetadata) o;

        return new EqualsBuilder()
                .append(id, mipMetadata.id)
                .append(cdmPath, mipMetadata.cdmPath)
                .append(imageName, mipMetadata.imageName)
                .append(imageArchivePath, mipMetadata.imageArchivePath)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(cdmPath)
                .append(imageName)
                .append(imageArchivePath)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("publishedName", publishedName)
                .append("libraryName", libraryName)
                .append("imageType", imageType)
                .append("cdmPath", cdmPath)
                .append("imageName", imageName)
                .append("imageArchivePath", imageArchivePath)
                .toString();
    }
}
