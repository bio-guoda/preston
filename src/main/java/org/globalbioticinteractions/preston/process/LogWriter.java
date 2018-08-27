package org.globalbioticinteractions.preston.process;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.cmd.CmdList;
import org.globalbioticinteractions.preston.model.RefNode;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Date;

public class LogWriter implements RefNodeListener {
    @Override
    public void on(RefNode refNode) {
        System.out.println(printDataset(refNode));
    }

    public static String printDataset(RefNode refNode) {
        String accessedAt = ISODateTimeFormat.dateTime().withZoneUTC().print(new Date().getTime());
        String parentId = refNode.getParent() == null ? "" : StringUtils.defaultString(refNode.getParent().getId());
        String id = StringUtils.defaultString(refNode.getId());
        String size = refNode.getSize() == null ? "" : refNode.getSize().toString();
        return (parentId + "\t" + id + "\t" + refNode.getLabel() + "\t" + refNode.getType().name() + "\t" + size + "\t" + accessedAt);
    }

}
