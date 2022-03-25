package org.janelia.colormipsearch.api.cdmips;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIdentityReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.api.JsonRequired;

@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class AbstractMetadata implements Serializable {
    private String id;
    private String publishedName;
    private String libraryName;
    private String cdmPath;
    private String imageName;
    private String imageArchivePath;
    private String imageType; // file or zipEntry
    private String imageURL;
    private String thumbnailURL;
    private String searchablePNG;
    // imageStack is defined for LM images
    private String imageStack;
    private String slideCode;
    private String driver;
    private String objective;
    // neuronType and neuronInstance are defined for EM images
    private String neuronType;
    private String neuronInstance;
    // use a default for the gender for now
    // LM should override this attribute and for EM will default to "female"
    private String gender = "f";
    private String anatomicalArea;
    private String alignmentSpace;
    private String channel;
    private String mountingProtocol;
    private Boolean publishedToStaging;
    private String relatedImageRefId;
    private String sampleRef;
    // variants are a mapping of the variant type, such as segmentation, gradient, zgap, gamma1_4,
    // to the corresponding image path
    @JsonProperty
    private Map<String, String> variants = null;

    @JsonRequired
    @JsonIdentityReference
    @JsonProperty
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @JsonRequired
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

    @JsonIgnore
    public String getCdmName() {
        if (StringUtils.isNotBlank(getCdmPath())) {
            return Paths.get(getCdmPath()).getFileName().toString();
        } else {
            return null;
        }
    }

    public String getImageName() {
        return imageName;
    }

    public void setImageName(String imageName) {
        this.imageName = imageName;
    }

    @JsonIgnore
    public String getImagePath() {
        if (StringUtils.isBlank(imageArchivePath)) {
            return imageName;
        } else {
            return Paths.get(imageArchivePath, imageName).toString();
        }
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

    @JsonRequired
    @JsonProperty
    public String getImageURL() {
        return imageURL;
    }

    public void setImageURL(String imageURL) {
        this.imageURL = imageURL;
    }

    @JsonRequired
    @JsonProperty
    public String getThumbnailURL() {
        return thumbnailURL;
    }

    public void setThumbnailURL(String thumbnailURL) {
        this.thumbnailURL = thumbnailURL;
    }

    @JsonRequired
    @JsonProperty
    public String getSearchablePNG() {
        return searchablePNG;
    }

    public void setSearchablePNG(String searchablePNG) {
        this.searchablePNG = searchablePNG;
    }

    public void setImageStack(String imageStack) {
        this.imageStack = imageStack;
    }

    public String getImageStack() {
        return imageStack;
    }

    @JsonRequired
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

    public boolean hasSlideCode() {
        return StringUtils.isNotBlank(slideCode);
    }

    @JsonIgnore
    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
    }

    public String getNeuronType() {
        return neuronType;
    }

    public void setNeuronType(String neuronType) {
        this.neuronType = neuronType;
    }

    public String getNeuronInstance() {
        return neuronInstance;
    }

    public void setNeuronInstance(String neuronInstance) {
        this.neuronInstance = neuronInstance;
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

    @JsonRequired
    @JsonProperty
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

    public String getRelatedImageRefId() {
        return relatedImageRefId;
    }

    public void setRelatedImageRefId(String relatedImageRefId) {
        this.relatedImageRefId = relatedImageRefId;
    }

    public String getSampleRef() {
        return sampleRef;
    }

    public void setSampleRef(String sampleRef) {
        this.sampleRef = sampleRef;
    }

    @JsonIgnore
    public Set<String> getVariantTypes() {
        if (variants == null) {
            return Collections.emptySet();
        } else {
            return variants.keySet();
        }
    }

    public boolean hasVariant(String variant) {
        return variants != null && StringUtils.isNotBlank(variants.get(variant));
    }

    public String getVariant(String variant) {
        if (hasVariant(variant)) {
            return variants.get(variant);
        } else {
            return null;
        }
    }

    public void addVariant(String variant, String variantLocation) {
        if (StringUtils.isBlank(variantLocation)) {
            return;
        }
        if (StringUtils.isBlank(variant)) {
            throw new IllegalArgumentException("Variant type for " + variantLocation + " cannot be blank");
        }
        if (variants == null) {
            variants = new LinkedHashMap<>();
        }
        variants.put(variant, variantLocation);
    }

    public <T extends AbstractMetadata> void copyVariantsFrom(T that) {
        that.getVariantTypes()
                .forEach(vt -> addVariant(vt, that.getVariant(vt)));
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

    public <T extends AbstractMetadata> void copyTo(T that) {
        that.setId(this.id);
        that.setPublishedName(this.publishedName);
        that.setAlignmentSpace(this.alignmentSpace);
        that.setLibraryName(this.libraryName);
        that.setImageURL(this.imageURL);
        that.setThumbnailURL(this.thumbnailURL);
        that.setSearchablePNG(this.getSearchablePNG());
        that.setImageStack(this.getImageStack());
        that.setCdmPath(this.getCdmPath());
        that.setImageType(this.getImageType());
        that.setImageName(this.getImageName());
        that.setImageArchivePath(this.getImageArchivePath());
        // copy the "attributes"
        that.setSampleRef(this.sampleRef);
        that.setSlideCode(this.slideCode);
        that.setObjective(this.objective);
        that.setNeuronType(this.neuronType);
        that.setNeuronInstance(this.neuronInstance);
        that.setGender(this.gender);
        that.setAnatomicalArea(this.anatomicalArea);
        that.setAlignmentSpace(this.alignmentSpace);
        that.setChannel(this.channel);
        that.setMountingProtocol(this.mountingProtocol);
        that.setPublishedToStaging(this.publishedToStaging);
    }

    protected Consumer<String> attributeValueHandler(String attrName) {
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
                case "neuronType":
                    return this::setNeuronType;
                case "neuronInstance":
                    return this::setNeuronInstance;
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
                case "searchableName":
                    return this::setSearchablePNG;
                case "imageStack":
                    return this::setImageStack;
                default:
                    return defaultAttributeValueHandler(attrName);
            }
        }
    }

    Consumer<String> defaultAttributeValueHandler(String attrName) {
        return (attrValue) -> {}; // do nothing
    }

    protected String mapAttr(String attrName) {
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
            case "neuronName":
                return "neuronName";
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
            case "searchableName":
                return "searchableName";
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
        ToStringBuilder builder = new ToStringBuilder(this)
                .append("id", id)
                .append("publishedName", publishedName);
        if (StringUtils.isNotBlank(sampleRef)) {
            builder.append("sampleRef", sampleRef);
        }
        if (StringUtils.isNotBlank(slideCode)) {
            builder.append("slideCode", slideCode);
        }
        if (StringUtils.isNotBlank(objective)) {
            builder.append("objective", objective);
        }
        if (StringUtils.isNotBlank(channel)) {
            builder.append("channel", channel);
        }
        return builder.append("libraryName", libraryName)
                .append("imageType", imageType)
                .append("cdmPath", cdmPath)
                .append("imageName", imageName)
                .append("imageArchivePath", imageArchivePath)
                .toString();
    }
}
