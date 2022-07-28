package org.janelia.colormipsearch.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * This is the representation of a CDS run.
 */
public class CDSSessionEntity extends AbstractSessionEntity {
    private List<String> masks = new ArrayList<>();
    private List<String> targets = new ArrayList<>();

    public List<String> getMasks() {
        return masks;
    }

    public void setMasks(List<String> masks) {
        this.masks = masks;
    }

    public void addMask(String m) {
        addTo(masks, m);
    }

    public List<String> getTargets() {
        return targets;
    }

    public void setTargets(List<String> targets) {
        this.targets = targets;
    }

    public void addTarget(String t) {
        addTo(targets, t);
    }

    private void addTo(List<String> l, String o) {
        if (StringUtils.isNotBlank(o)) {
            l.add(o);
        }
    }
}
