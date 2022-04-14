package bio.guoda.preston.cmd;

import com.beust.jcommander.converters.EnumConverter;

public class LoggerConverter extends EnumConverter<LogTypes> {
    public LoggerConverter(String optionName, Class<LogTypes> clazz) {
        super(optionName, clazz);
    }
}
