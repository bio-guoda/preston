package bio.guoda.preston.cmd;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

import java.net.URI;


public class URIValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
        try {
            URI.create(value);
        } catch (IllegalArgumentException ex) {
            throw new ParameterException("[" + value + "] should be an URI, but is not.", ex);
        }
    }
}

