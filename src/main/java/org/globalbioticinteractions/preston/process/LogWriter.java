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
    public void on(RefNodeRelation refNode) {
        System.out.println(printDataset(refNode));
    }

    public static String printDataset(RefNode refNode) {
        String accessedAt = ISODateTimeFormat.dateTime().withZoneUTC().print(new Date().getTime());
        String id = StringUtils.defaultString(refNode.getId());
        String size = refNode.getSize() == null ? "" : refNode.getSize().toString();
        return (id + "\t" + refNode.getLabel() + "\t" + refNode.getType().name() + "\t" + size + "\t" + accessedAt);
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
