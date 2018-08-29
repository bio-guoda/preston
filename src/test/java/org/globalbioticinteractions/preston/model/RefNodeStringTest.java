package org.globalbioticinteractions.preston.model;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RefNodeStringTest {

    @Test
    public void equivalent() {
        RefNode one = new RefNodeString("https://example.com");
        RefNode two = new RefNodeString("https://example.com");
        assertTrue(one.equivalentTo(two));
    }

    @Test
    public void differentValue() {
        RefNode one = new RefNodeString("https://example.com/2");
        RefNode two = new RefNodeString("https://example.com");
        assertFalse(one.equivalentTo(two));
    }

}