package org.globalbioticinteractions.preston.model;

import java.net.URI;

import static org.globalbioticinteractions.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static org.globalbioticinteractions.preston.RefNodeConstants.WAS_REVISION_OF;

public class RefNodeFactory {

    public static RefNode toUUID(String publisherUUID) {
        return new RefNodeString(publisherUUID);
    }

    public static RefNode toURI(String urlString) {
        return new RefNodeURI(URI.create(urlString));
    }

    public static RefNode toURI(URI uri) {
        return new RefNodeURI(uri);
    }

    public static RefNode toLiteral(String bla) {
        return new RefNodeString(bla);
    }

    public static RefNode toContentType(String contentType) {
        return new RefNodeString(contentType);
    }

    public static RefNode toDateTime(String dateTime) {
        return new RefNodeString(dateTime);
    }

    public static boolean isDerivedFrom(RefStatement statement) {
        return statement.getSubject() != null
                && statement.getObject() != null
                && statement.getPredicate() != null
                && (WAS_DERIVED_FROM.equivalentTo(statement.getPredicate())
                || WAS_REVISION_OF.equivalentTo(statement.getPredicate()));
    }

}
