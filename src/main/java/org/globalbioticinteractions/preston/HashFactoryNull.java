package org.globalbioticinteractions.preston;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

public class HashFactoryNull implements HashFactory {

    @Override
    public String hashFor(Dataset dataset) {
        try {
            return StringUtils.isBlank(dataset.getId())
                    ? CrawlerGBIF.calcSHA256(dataset.getData(), new NullOutputStream())
                    : dataset.getId();
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to process [" + dataset.getLabel() + "]", e);
        }
    }
}
