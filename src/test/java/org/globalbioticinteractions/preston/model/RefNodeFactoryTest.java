package org.globalbioticinteractions.preston.model;

import org.junit.Test;

public class RefNodeFactoryTest {

    @Test(expected = NullPointerException.class)
    public void createNullTriple() {
        RefNodeFactory.toStatement(null, null, null);
    }

}