package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefStatement;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class StatementHashLoggerTest {

    @Test
    public void relation() {
        RefNodeString source = new RefNodeString("source");
        RefNodeString relation = new RefNodeString("relation");
        RefNodeString target = new RefNodeString("target");

        String str = new StatementHashLogger().printStatement(new RefStatement(source, relation, target));

        assertThat(str, is("<hash://sha256/41cf6794ba4200b839c53531555f0f3998df4cbb01a4d5cb0b94e3ca5e23947d> <hash://sha256/fc8fbb48a3a16bfdd85345d0b6aa543ebd805c370e5b763ed75207185093fca3> <hash://sha256/34a04005bcaf206eec990bd9637d9fdb6725e0a0c0d4aebf003f17f4c956eb5c> ."));
    }


}