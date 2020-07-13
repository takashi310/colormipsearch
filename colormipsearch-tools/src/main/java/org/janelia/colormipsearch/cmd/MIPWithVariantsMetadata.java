package org.janelia.colormipsearch.cmd;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;

class MIPWithVariantsMetadata extends MIPMetadata {

    private Map<String, String> variants = null;
    private String sampleRef;

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
