package org.globalbioticinteractions.preston.model;

public class RefStatement {

    private final RefNode subject;
    private final RefNode predicate;
    private final RefNode object;

    RefStatement(RefNode subject, RefNode predicate, RefNode object) {
        this.predicate = predicate;
        this.subject = subject;
        this.object = object;
    }

    public String getLabel() {
        return "[" + (getSubject() == null ? "?" : getSubject().toString()) + "]-[:" + getPredicate().toString() + "]->[" + (getObject() == null ? "?" : getObject().toString()) + "]";
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
        return that.getObject() == getObject() || (getObject() != null && getObject().equals(that.getObject()));
    }

    private boolean equivalentSubject(RefStatement that) {
        return that.getSubject() == getSubject() || (getSubject() != null && getSubject().equals(that.getSubject()));
    }

    private boolean equivalentPredicate(RefStatement that) {
        return that.getPredicate() == getPredicate() || (getPredicate() != null && getPredicate().equals(that.getPredicate()));
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
