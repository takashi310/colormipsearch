package org.janelia.colormipsearch.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * This is the representation of a CDS run.
 */
public class CDSSessionEntity extends AbstractSessionEntity {
    private List<Map<String, Object>> masks = new ArrayList<>();
    private List<Map<String, Object>> targets = new ArrayList<>();

    public List<Map<String, Object>> getMasks() {
        return masks;
    }

    public void setMasks(List<Map<String, Object>> masks) {
        this.masks = masks;
    }

    public void addMask(Map<String, Object> m) {
        addTo(masks, m);
    }

    public List<Map<String, Object>> getTargets() {
        return targets;
    }

    public void setTargets(List<Map<String, Object>> targets) {
        this.targets = targets;
    }

    public void addTarget(Map<String, Object> t) {
        addTo(targets, t);
    }

    private void addTo(List<Map<String, Object>> l, Map<String, Object> m) {
        if (MapUtils.isNotEmpty(m)) {
            l.add(m);
        }
    }
}
