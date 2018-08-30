package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.converters.EnumConverter;

public class CrawlModeConverter extends EnumConverter<CrawlMode> {
    public CrawlModeConverter(String optionName, Class<CrawlMode> clazz) {
        super(optionName, clazz);
    }
}
