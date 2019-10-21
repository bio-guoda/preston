package bio.guoda.preston.cmd;

import bio.guoda.preston.store.KeyGeneratingStream;
import bio.guoda.preston.store.KeyTo1LevelPath;
import bio.guoda.preston.store.KeyTo3LevelPath;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreCopying;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.IRI;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

@Parameters(separators = "= ", commandDescription = "Copy biodiversity dataset graph")
public class CmdCopyTo extends LoggingPersisting implements Runnable {

    @Parameter(description = "[target directory]")
    private String targetDir;

    @Parameter(names = {"-t", "--type",}, description = "archive type", converter = ArchiveTypeConverter.class)
    private ArchiveType archiveType = ArchiveType.data_prov_provindex;

    protected ArchiveType getArchiveType() {
        return archiveType;
    }


    @Override
    public void run() {
        File source = getDefaultDataDir();
        File target = Persisting.getDataDir(targetDir);
        if (source.equals(target)) {
            throw new IllegalArgumentException("source dir [" + source.getAbsolutePath() + "] must be different from target dir [" + target.getAbsolutePath() + "].");
        }
        File tmp = getTmpDir();

        if (ArchiveType.data_prov_provindex.equals(getArchiveType())) {
            KeyValueStore copyingKeyValueStore = new KeyValueStoreCopying(
                    getKeyValueStore(),
                    new KeyValueStoreLocalFileSystem(tmp, new KeyTo3LevelPath(target.toURI())));
            CloneUtil.clone(copyingKeyValueStore);
        } else if (ArchiveType.data.equals(getArchiveType())) {
            KeyValueStore copyingKeyValueStoreBlob = new KeyValueStoreCopying(
                    getKeyValueStore(),
                    new KeyValueStoreLocalFileSystem(tmp, new KeyTo3LevelPath(target.toURI())));
            CloneUtil.clone(copyingKeyValueStoreBlob, getKeyValueStore(), getKeyValueStore());
        } else if (ArchiveType.prov.equals(getArchiveType())) {
            KeyValueStore copyingKeyValueStoreProv = new KeyValueStoreCopying(
                    getKeyValueStore(),
                    new KeyValueStoreLocalFileSystem(tmp, new KeyTo1LevelPath(target.toURI())));
            CloneUtil.clone(new NullKeyValueStore(), copyingKeyValueStoreProv, getKeyValueStore());
        } else if (ArchiveType.provindex.equals(getArchiveType())) {
            KeyValueStore copyingKeyValueStoreProv = new KeyValueStoreCopying(
                    getKeyValueStore(),
                    new KeyValueStoreLocalFileSystem(tmp, new KeyTo1LevelPath(target.toURI())));
            CloneUtil.clone(new NullKeyValueStore(), getKeyValueStore(), copyingKeyValueStoreProv);
        } else {
            throw new IllegalStateException("unsupport archive type [" + getArchiveType().name() + "]");
        }

    }

    private static class NullKeyValueStore implements KeyValueStore {

        @Override
        public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
            return null;
        }

        @Override
        public void put(IRI key, InputStream is) throws IOException {

        }

        @Override
        public InputStream get(IRI key) throws IOException {
            return null;
        }
    }
}
