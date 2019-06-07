package bio.guoda.preston.cmd;

import bio.guoda.preston.Resources;
import bio.guoda.preston.store.KeyTo1LevelPath;
import bio.guoda.preston.store.KeyTo3LevelPath;
import bio.guoda.preston.store.KeyTo5LevelPath;
import bio.guoda.preston.store.KeyToPath;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreCopying;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreRemoteHTTP;
import bio.guoda.preston.store.KeyValueStoreStickyFailover;
import bio.guoda.preston.store.KeyValueStoreWithFallback;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.URIConverter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class Persisting {

    @Parameter(names = {"--remote"}, description = "remote url", converter = URIConverter.class)
    private URI remoteURI = null;

    protected URI getRemoteURI() {
        return remoteURI;
    }

    protected boolean hasRemote() {
        return remoteURI != null;
    }

    KeyValueStore getKeyValueStore() {
        KeyValueStore store;
        if (hasRemote()) {
            KeyToPath keyToPath = new KeyTo1LevelPath(getRemoteURI());
            KeyValueStoreReadOnly keyStoreRemote1 = new KeyValueStoreRemoteHTTP(keyToPath, Resources::asInputStreamIgnore404);
            KeyToPath keyToPath2 = new KeyTo3LevelPath(getRemoteURI());
            KeyValueStoreReadOnly keyStoreRemote2 = new KeyValueStoreRemoteHTTP(keyToPath2, Resources::asInputStreamIgnore404);
            store = new KeyValueStoreCopying(
                    new KeyValueStoreStickyFailover(Arrays.asList(keyStoreRemote1, keyStoreRemote2)),
                    getKeyValueStoreLocal());
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

    public KeyValueStore getKeyValueStoreLocal() {
        KeyValueStore primary = new KeyValueStoreLocalFileSystem(getTmpDir(), getKeyToPathLocal());
        KeyValueStoreReadOnly fallback = new KeyValueStoreLocalFileSystem(getTmpDir(), new KeyTo5LevelPath(getDefaultDataDir().toURI()));
        return new KeyValueStoreWithFallback(primary, fallback);
    }

    KeyToPath getKeyToPathLocal() {
        return new KeyTo3LevelPath(getDefaultDataDir().toURI());
    }


}
