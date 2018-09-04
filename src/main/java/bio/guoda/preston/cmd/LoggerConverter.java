package bio.guoda.preston.cmd;

import com.beust.jcommander.converters.EnumConverter;

public class LoggerConverter extends EnumConverter<Logger> {
    public LoggerConverter(String optionName, Class<Logger> clazz) {
        super(optionName, clazz);
    }
}
