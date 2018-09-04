package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameter;
import org.apache.commons.io.FileUtils;
import org.globalbioticinteractions.preston.store.FilePersistence;

import java.io.File;
import java.io.IOException;

public class Persisting {

    @Parameter(names = {"-l", "--log",}, description = "select how to show the biodiversity graph", converter = LoggerConverter.class)
    private Logger logMode = Logger.nquads;

    protected Logger getLogMode() {
        return logMode;
    }


    FilePersistence getBlobPersistence() {
        return new FilePersistence(
                getTmpDir(),
                new File(getDataDir(), "blob"));
    }

    FilePersistence getStatementPersistence() {
        return new FilePersistence(getTmpDir(), new File(getDataDir(), "statement"));
    }


    File getTmpDir() {
        File tmp = new File(getDataDir(), "tmp");
        try {
            FileUtils.forceMkdir(tmp);
        } catch (IOException e) {
            //
        }
        return tmp;
    }

    File getDataDir() {
        File data = new File("data");
        try {
            FileUtils.forceMkdir(data);
        } catch (IOException e) {
            //
        }
        return data;
    }

}
