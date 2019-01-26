package bio.guoda.preston.cmd;

import bio.guoda.preston.store.FilePersistence;
import bio.guoda.preston.store.Persistence;
import com.beust.jcommander.Parameter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Persisting {

    Persistence getBlobPersistence() {
        return new FilePersistence(getTmpDir(), getDataDir());
    }

    Persistence getDatasetRelationsStore() {
        return new FilePersistence(getTmpDir(), getDataDir());
    }

    Persistence getLogRelationsStore() {
        return new FilePersistence(getTmpDir(), getDataDir());
    }

    File getTmpDir() {
        File tmp = new File("tmp");
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
