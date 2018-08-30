package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefStatement;
import org.joda.time.format.ISODateTimeFormat;

import java.net.URI;
import java.util.Date;

public class StatementHashLogger implements RefStatementListener {

    @Override
    public void on(RefStatement statement) {
        System.out.println(prependTime(printStatement(statement)));
    }

    String prependTime(String entry) {
        //String accessedAt = ISODateTimeFormat.dateTime().withZoneUTC().print(new Date().getTime());
        return entry;
    }

    protected String printStatement(RefStatement statement) {
        URI sourceId = statement.getSource().getContentHash();
        URI relationTypeId = statement.getRelationType().getContentHash();
        URI targetId = statement.getTarget().getContentHash();
        String relationId = statement.getId().toString();
        return "<" + sourceId + "> <" + relationTypeId + "> <" + targetId + "> .";
    }


}
