package bio.guoda.preston.cmd;

import com.beust.jcommander.converters.EnumConverter;

public class HashPathPatternConverter extends EnumConverter<ArchiveType> {
    public HashPathPatternConverter(String optionName, Class<ArchiveType> clazz) {
        super(optionName, clazz);
    }
}
