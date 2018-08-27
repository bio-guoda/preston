package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

import java.net.URI;
import java.util.Arrays;
import java.util.List;


public class URIValidator implements IParameterValidator {

    private final List<String> list = Arrays.asList("json", "tsv");

    @Override
    public void validate(String name, String value) throws ParameterException {
        try {
            URI.create(value);
        } catch (IllegalArgumentException ex) {
            throw new ParameterException("[" + value + "] should be an URI, but is not.", ex);
        }
    }
}

