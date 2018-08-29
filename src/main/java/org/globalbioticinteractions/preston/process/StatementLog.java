package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefStatement;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Date;

public class StatementLog extends StatementHashLog  {

    @Override
    String printStatement(RefStatement statement) {
        return statement.getSource().getLabel() + "\t" + statement.getRelationType().getLabel() + "\t" + statement.getTarget().getLabel();
    }


}
