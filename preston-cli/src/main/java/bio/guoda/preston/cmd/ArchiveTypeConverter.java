package bio.guoda.preston.cmd;

import com.beust.jcommander.converters.EnumConverter;

public class ArchiveTypeConverter extends EnumConverter<ArchiveType> {
    public ArchiveTypeConverter(String optionName, Class<ArchiveType> clazz) {
        super(optionName, clazz);
    }
}
