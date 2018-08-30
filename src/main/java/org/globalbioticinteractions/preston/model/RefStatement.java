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

    public boolean equivalentTo(RefStatement that) {
        if (that == null) {
            throw new IllegalArgumentException("that cannot be null");
        }
        return equivalentSubject(that)
                && equivalentPredicate(that)
                && equivalentObject(that);
    }

    private boolean equivalentObject(RefStatement that) {
        return that.getObject() == getObject() || (getObject() != null && getObject().equivalentTo(that.getObject()));
    }

    private boolean equivalentSubject(RefStatement that) {
        return that.getSubject() == getSubject() || (getSubject() != null && getSubject().equivalentTo(that.getSubject()));
    }

    private boolean equivalentPredicate(RefStatement that) {
        return that.getPredicate() == getPredicate() || (getPredicate() != null && getPredicate().equivalentTo(that.getPredicate()));
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
