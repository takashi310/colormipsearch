package org.janelia.colormipsearch.api.pppsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PPPImageVariant {
    public enum VariantType {
        RAW("_1_raw.png"),
        MASKED_RAW("_2_masked_raw.png"),
        SKEL("_3_skel.png"),
        MASKED_INST("_4_masked_inst.png"),
        CH("_5_ch.png"),
        CH_SKEL("_6_ch_skel.png");

        private String suffix;

        VariantType(String suffix) {
            this.suffix = suffix;
        }

        public static VariantType findVariantType(String imageName) {
            for (VariantType vt : values()) {
                if (imageName.endsWith(vt.suffix)) {
                    return vt;
                }
            }
            return null;
        }
    }

    private final VariantType variantType;
    private final String imagePath;

    @JsonCreator
    public PPPImageVariant(@JsonProperty VariantType variantType, @JsonProperty String imagePath) {
        this.variantType = variantType;
        this.imagePath = imagePath;
    }

    public VariantType getVariantType() {
        return variantType;
    }

    public String getImagePath() {
        return imagePath;
    }

}
