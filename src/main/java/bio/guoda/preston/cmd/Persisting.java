package bio.guoda.preston.cmd;

import com.beust.jcommander.Parameter;
import org.apache.commons.io.FileUtils;
import bio.guoda.preston.store.FilePersistence;

import java.io.File;
import java.io.IOException;

public class Persisting {

    FilePersistence getBlobPersistence() {
        return new FilePersistence(getTmpDir(), getDataDir());
    }

    FilePersistence getStatementPersistence() {
        return new FilePersistence(getTmpDir(), getDataDir());
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
