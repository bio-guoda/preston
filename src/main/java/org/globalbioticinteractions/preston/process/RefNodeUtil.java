package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefStatement;

import static org.globalbioticinteractions.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static org.globalbioticinteractions.preston.RefNodeConstants.WAS_REVISION_OF;

public class RefNodeUtil {

    public static RefNodeString toUUID(String publisherUUID) {
        return new RefNodeString(publisherUUID);
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
