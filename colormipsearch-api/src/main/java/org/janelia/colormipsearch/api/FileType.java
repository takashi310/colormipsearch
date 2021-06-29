package org.janelia.colormipsearch.api;

public enum FileType {
    SignalMip("_1_raw.png"),
    SignalMipMasked("_2_masked_raw.png"),
    SignalMipMaskedSkel("_3_skel.png"),
    ColorDepthMip("_5_ch.png"),
    ColorDepthMipSkel("_6_ch_skel.png");

    private String suffix;

    FileType(String suffix) {
        this.suffix = suffix;
    }

    public static FileType findFileType(String fname) {
        for (FileType vt : values()) {
            if (fname.endsWith(vt.suffix)) {
                return vt;
            }
        }
        return null;
    }
}
