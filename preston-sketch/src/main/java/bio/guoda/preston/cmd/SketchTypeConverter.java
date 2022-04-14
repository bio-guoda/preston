package bio.guoda.preston.cmd;

import com.beust.jcommander.converters.EnumConverter;

public class SketchTypeConverter extends EnumConverter<SketchType> {
    public SketchTypeConverter(String optionName, Class<SketchType> clazz) {
        super(optionName, clazz);
    }
}
