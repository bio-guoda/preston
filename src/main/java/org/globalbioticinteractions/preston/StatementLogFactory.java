package org.globalbioticinteractions.preston;

import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.cmd.Logger;
import org.globalbioticinteractions.preston.process.StatementListener;
import org.globalbioticinteractions.preston.process.StatementLoggerNQuads;
import org.globalbioticinteractions.preston.process.StatementLoggerTSV;

import java.util.concurrent.atomic.AtomicLong;

public class StatementLogFactory {
    public static StatementListener createLogger(Logger logMode) {
        StatementListener logger;
        if (Logger.tsv == logMode) {
            logger = new StatementLoggerTSV();
        } else if (Logger.nquads == logMode) {
            logger = new StatementLoggerNQuads();
        } else {
            logger = new StatementListener() {
                AtomicLong count = new AtomicLong(1);

                @Override
                public void on(Triple statement) {
                    long index = count.getAndIncrement();
                    if ((index % 80) == 0) {
                        System.out.println();
                    } else {
                        System.out.print(".");
                    }
                }
            };
        }
        return logger;
    }
}
