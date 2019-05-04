package bio.guoda.preston.cmd;

import bio.guoda.preston.store.KeyTo5LevelPath;
import bio.guoda.preston.store.KeyValueStoreCopying;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreRemoteHTTP;
import bio.guoda.preston.store.KeyValueStoreWithFallback;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.URLConverter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class Persisting {

    @Parameter(names = {"--remote"}, description = "remote url", converter = URLConverter.class)
    private URL remoteURL = null;

    protected URL getRemoteURL() {
        return remoteURL;
    }

    protected boolean hasRemote() {
        return remoteURL != null;
    }

    KeyValueStore getKeyValueStore() {
        KeyValueStore store;
        if (hasRemote()) {
            store = new KeyValueStoreCopying(new KeyValueStoreRemoteHTTP(getRemoteURL()), getKeyValueStoreLocal());
        } else {
            store = getKeyValueStoreLocal();
        }
        return store;
    }


    File getDefaultDataDir() {
        return getDataDir("data");
    }

    File getTmpDir() {
        return getDataDir("tmp");
    }

    static File getDataDir(String data1) {
        File data = new File(data1);
        try {
            FileUtils.forceMkdir(data);
        } catch (IOException e) {
            //
        }
        return data;
    }

    KeyValueStore getKeyValueStoreLocal() {
        KeyValueStore primary = new KeyValueStoreLocalFileSystem(getTmpDir(), getDefaultDataDir());
        KeyValueStoreReadOnly fallback = new KeyValueStoreLocalFileSystem(getTmpDir(), getDefaultDataDir(), new KeyTo5LevelPath());
        return new KeyValueStoreWithFallback(primary, fallback);
    }



}
