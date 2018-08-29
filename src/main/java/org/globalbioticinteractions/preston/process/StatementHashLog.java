package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefStatement;
import org.joda.time.format.ISODateTimeFormat;

import java.net.URI;
import java.util.Date;

public class StatementHashLog implements RefNodeListener {

    @Override
    public void on(RefStatement relation) {
        System.out.println(prependTime(printStatement(relation)));
    }

    String prependTime(String entry) {
        String accessedAt = ISODateTimeFormat.dateTime().withZoneUTC().print(new Date().getTime());
        return accessedAt + "\t" + entry;
    }

    String printStatement(RefStatement statement) {
        URI sourceId = statement.getSource().getContentHash();
        URI relationTypeId = statement.getRelationType().getContentHash();
        URI targetId = statement.getTarget().getContentHash();
        String relationId = statement.getId().toString();
        return relationId + "\t" + sourceId + "\t" + relationTypeId + "\t" + targetId;
    }


}
