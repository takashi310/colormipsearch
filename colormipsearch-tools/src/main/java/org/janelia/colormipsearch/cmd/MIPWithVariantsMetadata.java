package org.janelia.colormipsearch.cmd;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;

class MIPWithVariantsMetadata extends MIPMetadata {

    private Map<String, String> variants = null;
    private String sampleRef;

    Set<String> getVariantTypes() {
        if (variants == null) {
            return Collections.emptySet();
        } else {
            return variants.keySet();
        }
    }

    boolean hasVariant(String variant) {
        return variants != null && StringUtils.isNotBlank(variants.get(variant));
    }
    String getVariant(String variant) {
        if (hasVariant(variant)) {
            return variants.get(variant);
        } else {
            return null;
        }
    }

    MIPMetadata variantAsMIP(String variant) {
        if (hasVariant(variant)) {
            MIPMetadata variantMIP = new MIPMetadata();
            variantMIP.setImageName(variants.get(variant));
            variantMIP.setImageArchivePath(variants.get(variant + "ArchivePath"));
            variantMIP.setImageType(variants.get(variant + "EntryType"));
            return variantMIP;
        } else {
            return null;
        }
    }

    void addVariant(String variant, String variantLocation, String variantName, String variantEntryType) {
        if (StringUtils.isBlank(variantName)) {
            return;
        }
        Preconditions.checkArgument(StringUtils.isNotBlank(variant));
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

    @JsonProperty
    Map<String, String> getVariants() {
        return variants;
    }

    @JsonProperty
    public String getSampleRef() {
        return sampleRef;
    }

    public void setSampleRef(String sampleRef) {
        this.sampleRef = sampleRef;
    }
}
