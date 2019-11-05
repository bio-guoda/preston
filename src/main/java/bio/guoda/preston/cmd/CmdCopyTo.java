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
            copyAll(target, tmp);
        } else if (ArchiveType.data.equals(getArchiveType())) {
            copyDataOnly(target, tmp);
        } else if (ArchiveType.prov.equals(getArchiveType())) {
            copyProvLogsOnly(target, tmp);
        } else if (ArchiveType.provindex.equals(getArchiveType())) {
            copyProvIndexOnly(target, tmp);
        } else {
            throw new IllegalStateException("unsupport archive type [" + getArchiveType().name() + "]");
        }

    }

    public void copyAll(File target, File tmp) {
        KeyValueStore copyingKeyValueStore = new KeyValueStoreCopying(
                getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()),
                new KeyValueStoreLocalFileSystem(tmp, new KeyTo3LevelPath(target.toURI()), new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()));

        KeyValueStore copyingKeyValueStoreIndex = new KeyValueStoreCopying(
                getKeyValueStore(new KeyValueStoreLocalFileSystem.KeyValueStreamFactorySHA256Values()),
                new KeyValueStoreLocalFileSystem(tmp, new KeyTo3LevelPath(target.toURI()), new KeyValueStoreLocalFileSystem.KeyValueStreamFactorySHA256Values()));

        CloneUtil.clone(copyingKeyValueStore, copyingKeyValueStore, copyingKeyValueStoreIndex);
    }

    private void copyProvIndexOnly(File target, File tmp) {
        KeyValueStore copyingKeyValueStoreProv = new KeyValueStoreCopying(
                getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()),
                new KeyValueStoreLocalFileSystem(tmp, new KeyTo1LevelPath(target.toURI()), new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()));

        CloneUtil.clone(
                new NullKeyValueStore(),
                getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()),
                copyingKeyValueStoreProv
        );
    }

    private void copyProvLogsOnly(File target, File tmp) {
        KeyValueStore copyingKeyValueStoreProv = new KeyValueStoreCopying(
                getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()),
                new KeyValueStoreLocalFileSystem(tmp, new KeyTo1LevelPath(target.toURI()), new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()));

        CloneUtil.clone(
                new NullKeyValueStore(),
                copyingKeyValueStoreProv,
                getKeyValueStore(new KeyValueStoreLocalFileSystem.KeyValueStreamFactorySHA256Values())
        );
    }

    private void copyDataOnly(File target, File tmp) {
        KeyValueStore copyingKeyValueStoreBlob = new KeyValueStoreCopying(
                getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()),
                new KeyValueStoreLocalFileSystem(tmp,
                        new KeyTo3LevelPath(target.toURI()),
                        new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()));

        CloneUtil.clone(
                copyingKeyValueStoreBlob,
                getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()),
                getKeyValueStore(new KeyValueStoreLocalFileSystem.KeyValueStreamFactorySHA256Values()));
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
