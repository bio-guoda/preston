package bio.guoda.preston.cmd;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class RegexValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
        try {
            Pattern.compile(value);
        } catch (PatternSyntaxException ex) {
            throw new ParameterException("[" + value + "] should be an IRI, but is not.", ex);
        }
    }
}

