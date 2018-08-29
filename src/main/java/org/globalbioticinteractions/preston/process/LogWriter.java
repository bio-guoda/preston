package org.globalbioticinteractions.preston.process;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.cmd.CmdList;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeRelation;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Date;

public class LogWriter implements RefNodeListener {
    @Override
    public void on(RefNodeRelation relation) {
        System.out.println(printRelation(relation));
    }

    public static String printRelation(RefNodeRelation refNode) {
        String accessedAt = ISODateTimeFormat.dateTime().withZoneUTC().print(new Date().getTime());
        String sourceId = StringUtils.defaultString(refNode.getSource().getId());
        String relationTypeId = StringUtils.defaultString(refNode.getRelationType().getId());
        String targetId = StringUtils.defaultString(refNode.getTarget().getId());
        String relationId = StringUtils.defaultString(refNode.getId());
        return relationId + "\t" + sourceId + "\t" + relationTypeId + "\t" + targetId + "\t" + refNode.getLabel() + "\t" + accessedAt;
    }

}
