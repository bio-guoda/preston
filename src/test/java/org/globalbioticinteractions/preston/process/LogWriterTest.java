package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefNodeRelation;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringStartsWith.startsWith;

public class LogWriterTest {

    @Test
    public void relation() {
        RefNodeString source = new RefNodeString("source");
        RefNodeString relation = new RefNodeString("relation");
        RefNodeString target = new RefNodeString("target");

        String str = LogWriter.printRelation(new RefNodeRelation(source, relation, target));

        assertThat(str, startsWith("ce0d8e5c8ac18cb76b418972c7882f0b94e0bc2c952130e0511a1aa28c2ac9e0\t41cf6794ba4200b839c53531555f0f3998df4cbb01a4d5cb0b94e3ca5e23947d\tfc8fbb48a3a16bfdd85345d0b6aa543ebd805c370e5b763ed75207185093fca3\t34a04005bcaf206eec990bd9637d9fdb6725e0a0c0d4aebf003f17f4c956eb5c\t[source]-[:relation]->[target]"));
    }


}