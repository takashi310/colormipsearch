package org.janelia.colormipsearch.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.janelia.colormipsearch.model.annotations.EntityId;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AbstractBaseEntity implements BaseEntity {

    @EntityId
    private Number entityId;

    @Override
    public Number getEntityId() {
        return entityId;
    }

    @Override
    public void setEntityId(Number entityId) {
        this.entityId = entityId;
    }

    @Override
    public boolean hasEntityId() {
        return entityId != null;
    }
}
