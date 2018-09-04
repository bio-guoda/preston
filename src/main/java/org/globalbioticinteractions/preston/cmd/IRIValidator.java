package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import org.globalbioticinteractions.preston.model.RefNodeFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.List;


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

