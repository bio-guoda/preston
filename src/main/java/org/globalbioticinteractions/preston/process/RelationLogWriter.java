package org.globalbioticinteractions.preston.process;

import org.apache.commons.lang3.StringUtils;
import org.globalbioticinteractions.preston.model.RefNodeRelation;
import org.joda.time.format.ISODateTimeFormat;

import java.net.URI;
import java.util.Date;

public class RelationLogWriter implements RefNodeListener {
    @Override
    public void on(RefNodeRelation relation) {
        System.out.println(printRelation(relation));
    }

    public static String printRelation(RefNodeRelation refNode) {
        String accessedAt = ISODateTimeFormat.dateTime().withZoneUTC().print(new Date().getTime());
        URI sourceId = refNode.getSource().getId();
        URI relationTypeId = refNode.getRelationType().getId();
        URI targetId = refNode.getTarget().getId();
        String relationId = refNode.getId().toString();
        return relationId + "\t" + sourceId + "\t" + relationTypeId + "\t" + targetId + "\t" + refNode.getLabel() + "\t" + accessedAt;
    }


}
