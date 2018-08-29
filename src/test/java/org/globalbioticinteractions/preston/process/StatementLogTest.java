package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefStatement;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringStartsWith.startsWith;

public class StatementLogTest {

    @Test
    public void relation() {
        RefNodeString source = new RefNodeString("source");
        RefNodeString relation = new RefNodeString("relation");
        RefNodeString target = new RefNodeString("target");

        String str = new StatementLog().printStatement(new RefStatement(source, relation, target));

        assertThat(str, is("source\trelation\ttarget"));
    }


}