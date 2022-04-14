package bio.guoda.preston.cmd;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import bio.guoda.preston.RefNodeFactory;


public class IRIValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
        try {
            RefNodeFactory.toIRI(value);
        } catch (IllegalArgumentException ex) {
            throw new ParameterException("[" + value + "] should be an IRI, but is not.", ex);
        }
    }
}

