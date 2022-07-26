package org.janelia.colormipsearch.model;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.janelia.colormipsearch.model.annotations.EntityId;

/**
 * Common attributes for the database persisted data.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AbstractBaseEntity implements BaseEntity {

    @EntityId
    private Number entityId;
    private Date createdDate = new Date();

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

    @Override
    public Date getCreatedDate() {
        return createdDate;
    }

    @Override
    public void setCreatedDate(Date createdDate) {
        this.createdDate = createdDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        AbstractBaseEntity that = (AbstractBaseEntity) o;

        return new EqualsBuilder().append(entityId, that.entityId).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(entityId).toHashCode();
    }
}
