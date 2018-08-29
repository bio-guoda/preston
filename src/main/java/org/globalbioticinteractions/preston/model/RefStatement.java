package org.globalbioticinteractions.preston.model;

import org.apache.commons.io.IOUtils;
import org.globalbioticinteractions.preston.Hasher;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class RefStatement {

    private final RefNode source;
    private final RefNode target;
    private final RefNode relationType;
    private URI id;
    
    public RefStatement(RefNode source, RefNode relationType, RefNode target) {
        this.relationType = relationType;
        this.source = source;
        this.target = target;
    }

    public InputStream getData() throws IOException {
        return IOUtils.toInputStream(getDataString(), StandardCharsets.UTF_8);
    }

    private String getDataString() {
        String sourceId = getSource() == null ? "" : getSource().getContentHash().toString();
        String relationshipTypeId = getRelationType() == null ? "" : getRelationType().getContentHash().toString();
        String targetId = getTarget() == null ? "" : getTarget().getContentHash().toString();
        return sourceId + relationshipTypeId + targetId;
    }

    public URI getId() {
        if (this.id == null) {
            this.id = Hasher.calcSHA256(getDataString());
        }
        return id;
    }

    public String getLabel() {
        return "[" + getSource().getLabel() + "]-[:" + getRelationType().getLabel() + "]->[" + (getTarget() == null ? "?" : getTarget().getLabel()) + "]";
    }

    public boolean equivalentTo(RefStatement other) {
        return (getTarget().equivalentTo(other.getTarget()))
                && (getRelationType().equivalentTo(other.getRelationType()))
                && (getSource().equivalentTo(other.getSource()));
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
