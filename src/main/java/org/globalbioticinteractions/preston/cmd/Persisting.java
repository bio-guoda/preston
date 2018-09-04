package org.globalbioticinteractions.preston.cmd;

import org.globalbioticinteractions.preston.store.FilePersistence;

import java.io.File;

public class Persisting {

    FilePersistence getBlobPersistence() {
        return new FilePersistence(
                getTmpDir(),
                new File(getDataDir(), "blob"));
    }

    File getTmpDir() {
        return new File(getDataDir(), "tmp");
    }

    File getDataDir() {
        return new File("data");
    }

}
