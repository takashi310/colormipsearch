package org.janelia.colormipsearch.api.cdmips;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;

public class MIPMetadata extends AbstractMetadata {
    private String relatedImageRefId;
    private String sampleRef;
    // variants are a mapping of the variant type, such as segmentation, gradient, zgap, gamma1_4,
    // to the corresponding image path
    @JsonProperty
    private Map<String, String> variants = null;

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
    public String getCdmName() {
        if (StringUtils.isNotBlank(getCdmPath())) {
            return Paths.get(getCdmPath()).getFileName().toString();
        } else {
            return null;
        }
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

    public MIPMetadata variantAsMIP(String variant) {
        if (hasVariant(variant)) {
            MIPMetadata variantMIP = new MIPMetadata();
            variantMIP.setId(getId());
            variantMIP.setCdmPath(getCdmPath());
            variantMIP.setImageName(variants.get(variant));
            variantMIP.setImageArchivePath(variants.get(variant + "ArchivePath"));
            variantMIP.setImageType(variants.get(variant + "EntryType"));
            return variantMIP;
        } else {
            return null;
        }
    }

    public void addVariant(String variant, String variantLocation, String variantName, String variantEntryType) {
        if (StringUtils.isBlank(variantName)) {
            return;
        }
        if (StringUtils.isBlank(variant)) {
            throw new IllegalArgumentException("Variant type for " + variantName + " cannot be blank");
        }
        if (variants == null) {
            variants = new LinkedHashMap<>();
        }
        if (StringUtils.equalsIgnoreCase(variantEntryType, "zipEntry")) {
            variants.put(variant, variantName);
            variants.put(variant + "ArchivePath", variantLocation);
            variants.put(variant + "EntryType", "zipEntry");
        } else {
            if (StringUtils.isNotBlank(variantLocation)) {
                Path variantPath  = Paths.get(variantLocation, variantName);
                variants.put(variant, variantPath.toString());
            } else {
                variants.put(variant, variantName);
            }
        }
    }

}
