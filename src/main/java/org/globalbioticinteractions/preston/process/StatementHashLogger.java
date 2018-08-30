package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefStatement;

import java.net.URI;

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
        URI sourceId = statement.getSubject().getContentHash();
        URI relationTypeId = statement.getPredicate().getContentHash();
        URI targetId = statement.getObject().getContentHash();
        String relationId = statement.getId().toString();
        return "<" + sourceId + "> <" + relationTypeId + "> <" + targetId + "> .";
    }


}
