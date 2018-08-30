package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeString;

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
}
