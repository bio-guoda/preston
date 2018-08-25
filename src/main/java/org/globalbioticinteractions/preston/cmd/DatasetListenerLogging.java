package org.globalbioticinteractions.preston.cmd;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.Dataset;
import org.globalbioticinteractions.preston.DatasetListener;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Date;

public class DatasetListenerLogging implements DatasetListener {
    private static Log LOG = LogFactory.getLog(CmdList.class);

    @Override
    public void onDataset(Dataset dataset) {
            System.out.println(printDataset(dataset));
    }

    static String printDataset(Dataset dataset) {
        String accessedAt = ISODateTimeFormat.dateTime().withZoneUTC().print(new Date().getTime());
        String parentId = dataset.getParent() == null ? "" : StringUtils.defaultString(dataset.getParent().getId());
        String id = StringUtils.defaultString(dataset.getId());
        return (parentId + "\t" + id + "\t" + dataset.getLabel() + "\t" + dataset.getType().name() + "\t" + accessedAt);
    }

}
