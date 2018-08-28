package org.globalbioticinteractions.preston.model;

import org.apache.commons.io.IOUtils;
import org.globalbioticinteractions.preston.Hasher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class RefNodeRelation extends RefNodeImpl {

    private final RefNode source;
    private final RefNode target;
    private final RefNode relationType;
    private String id;
    
    public RefNodeRelation(RefNode source, RefNode relationType, RefNode target) {
        super(RefNodeType.RELATION);
        this.relationType = relationType;
        if (relationType == null) {
            throw new IllegalArgumentException("null relation");
        }
        this.source = source;
        if (source == null) {
            throw new IllegalArgumentException("null source");
        }
        this.target = target;
        if (target == null) {
            throw new IllegalArgumentException("null target");
        }
    }

    @Override
    public InputStream getData() throws IOException {
        return IOUtils.toInputStream(getDataString(), StandardCharsets.UTF_8);
    }

    private String getDataString() {
        String sourceId = getSource().getId();
        String relationshipTypeId = getRelationType().getId();
        String targetId = getTarget().getId();
        if (sourceId == null) {
            throw new IllegalArgumentException("null source for [" + getLabel() + "]");
        }
        if (relationshipTypeId == null) {
            throw new IllegalArgumentException("null relation for [" + getLabel() + "]");
        }
        if (targetId == null) {
            throw new IllegalArgumentException("null target for [" + getLabel() + "]");
        }
        return sourceId + "<-[" + relationshipTypeId + "]-" + targetId;
    }

    @Override
    public String getId() {
        if (this.id == null) {
            this.id = Hasher.calcSHA256(getDataString());
        }
        return id;
    }

    @Override
    public Long getSize() {
        return (long) getDataString().getBytes(StandardCharsets.UTF_8).length;
    }

    @Override
    public String getLabel() {
        return "[" + getSource().getLabel() + "]<-[:" + getRelationType().getLabel() + "]-[" + getTarget().getLabel() + "]";
    }

    @Override
    public boolean equivalentTo(RefNode other) {
        return other instanceof RefNodeRelation
                && Objects.equals(getType(), other.getType())
                && (getTarget().equivalentTo(((RefNodeRelation) other).getTarget()))
                && (getRelationType().equivalentTo(((RefNodeRelation) other).getRelationType()))
                && (getSource().equivalentTo(((RefNodeRelation) other).getSource()));
    }

    public RefNode getTarget() {
        return target;
    }

    public RefNode getRelationType() {
        return relationType;
    }

    public RefNode getSource() {
        return source;
    }
}
