package org.globalbioticinteractions.preston.process;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeImpl;
import org.globalbioticinteractions.preston.model.RefNodeType;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class RefNodeRelation extends RefNodeImpl {

    private final RefNode target;
    private final RefNode relationType;

    public RefNodeRelation(RefNode source, RefNode relationType, RefNode target) {
        super(source, RefNodeType.RELATION);
        this.relationType = relationType;
        this.target = target;
    }

    @Override
    public InputStream getData() throws IOException {
        String sourceId = getParent() == null ? null : getParent().getId();
        String relationshipTypeId = getRelationType() == null ? null : getRelationType().getId();
        String targetId = getTarget() == null ? null : getTarget().getId();
        return StringUtils.isNotBlank(sourceId) && StringUtils.isNotBlank(targetId)
                ? IOUtils.toInputStream(sourceId + "<-[" + relationshipTypeId + "]-" + targetId, StandardCharsets.UTF_8)
                : null;
    }

    @Override
    public String getLabel() {
        return "[" + getParent().getLabel() + "]<-[:"+ getRelationType().getLabel() + "]-[" + getTarget().getLabel() + "]";
    }

    @Override
    public boolean equivalentTo(RefNode other) {
        return other instanceof RefNodeRelation
                && Objects.equals(getType(), other.getType())
                && (getTarget() != null && getTarget().equivalentTo(((RefNodeRelation) other).getTarget()))
                && (getRelationType() != null && getRelationType().equivalentTo(((RefNodeRelation) other).getRelationType()))
                && (getParent() != null && getParent().equivalentTo(other.getParent()));
    }

    public RefNode getTarget() {
        return target;
    }

    public RefNode getRelationType() {
        return relationType;
    }
}
