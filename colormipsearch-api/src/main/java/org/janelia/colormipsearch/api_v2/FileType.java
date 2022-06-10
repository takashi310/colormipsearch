package org.janelia.colormipsearch.api_v2;

public enum FileType {
    SignalMip("_1_raw.png"),
    SignalMipMasked("_2_masked_raw.png"),
    SignalMipMaskedSkel("_3_skel.png"),
    ColorDepthMip("_5_ch.png"),
    ColorDepthMipSkel("_6_ch_skel.png");

    private String fullSuffix;

    FileType(String fullSuffix) {
        this.fullSuffix = fullSuffix;
    }

    public static FileType findFileType(String fname) {
        for (FileType vt : values()) {
            if (fname.endsWith(vt.fullSuffix)) {
                return vt;
            }
        }
        return null;
    }

    public String getFullSuffix() {
        return fullSuffix;
    }

    public String getDisplaySuffix() {
        return fullSuffix.substring(3); // hacky to remove the prefix _n_ but it works for now
    }
}
