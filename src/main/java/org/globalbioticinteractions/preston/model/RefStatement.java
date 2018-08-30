package org.globalbioticinteractions.preston.model;

import org.apache.commons.io.IOUtils;
import org.globalbioticinteractions.preston.Hasher;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class RefStatement {

    private final RefNode subject;
    private final RefNode predicate;
    private final RefNode object;
    private URI id;
    
    public RefStatement(RefNode subject, RefNode predicate, RefNode object) {
        this.predicate = predicate;
        this.subject = subject;
        this.object = object;
    }

    public InputStream getData() throws IOException {
        return IOUtils.toInputStream(getDataString(), StandardCharsets.UTF_8);
    }

    private String getDataString() {
        String sourceId = getSubject() == null ? "" : getSubject().getContentHash().toString();
        String relationshipTypeId = getPredicate() == null ? "" : getPredicate().getContentHash().toString();
        String targetId = getObject() == null ? "" : getObject().getContentHash().toString();
        return sourceId + relationshipTypeId + targetId;
    }

    public URI getId() {
        if (this.id == null) {
            this.id = Hasher.calcSHA256(getDataString());
        }
        return id;
    }

    public String getLabel() {
        return "[" + (getSubject() == null ? "?" : getSubject().getLabel()) + "]-[:" + getPredicate().getLabel() + "]->[" + (getObject() == null ? "?" : getObject().getLabel()) + "]";
    }

    public boolean equivalentTo(RefStatement other) {
        return (getObject().equivalentTo(other.getObject()))
                && (getPredicate().equivalentTo(other.getPredicate()))
                && (getSubject().equivalentTo(other.getSubject()));
    }

    public RefNode getObject() {
        return object;
    }

    public RefNode getPredicate() {
        return predicate;
    }

    public RefNode getSubject() {
        return subject;
    }
}
