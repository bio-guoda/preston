package bio.guoda.preston.cmd;

import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.HexaStoreConstants;
import bio.guoda.preston.store.KeyGeneratingStream;
import bio.guoda.preston.store.KeyTo1LevelPath;
import bio.guoda.preston.store.KeyTo3LevelPath;
import bio.guoda.preston.store.KeyToPath;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreCopying;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.HexaStore;
import bio.guoda.preston.store.HexaStoreImpl;
import bio.guoda.preston.util.JekyllUtil;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.IRI;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;

@Parameters(separators = "= ", commandDescription = "Copy biodiversity dataset graph")
public class CmdCopyTo extends LoggingPersisting implements Runnable {

    @Parameter(description = "[target directory]")
    private String targetDir;

    @Parameter(names = {"-t", "--type",}, description = "archive type", converter = ArchiveTypeConverter.class)
    private ArchiveType archiveType = ArchiveType.data_prov_provindex;

    @Parameter(names = {"-p", "--target-hash-path-pattern",}, description = "hash path pattern of content to be copied", converter = HashPathPatternConverter.class)
    private HashPathPattern pathPattern = HashPathPattern.directoryDepth2;

    protected ArchiveType getArchiveType() {
        return archiveType;
    }


    @Override
    public void run() {
        File source = getDefaultDataDir();
        File target = Persisting.getDataDir(targetDir);
        if (ArchiveType.jekyll.equals(getArchiveType())) {
            generateJekyllSiteContent(target);
        } else {
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

    }

    private void generateJekyllSiteContent(File target) {
        final BlobStoreAppendOnly provenanceLogStore
                = new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())), true, getHashType());
        final StatementsListener listener;
        try {
            listener = JekyllUtil.createJekyllSiteGenerator(provenanceLogStore, target);
        } catch (IOException e) {
            throw new RuntimeException("failed to create jekyll site content", e);
        }

        final AtomicReference<DateTime> lastCrawlTime = new AtomicReference<>();
        final CmdContext ctx = new CmdContext(this, listener, JekyllUtil.createPrestonStartTimeListener(lastCrawlTime::set));

        final HexaStore hexastore = new HexaStoreImpl(
                getKeyValueStore(new KeyValueStoreLocalFileSystem.KeyValueStreamFactoryValues(getHashType())), getHashType());


        attemptReplay(
                provenanceLogStore,
                hexastore,
                ctx);

        JekyllUtil.writePrestonConfigFile(target, lastCrawlTime, hexastore, getProvenanceRoot());
    }

    private void copyAll(File target, File tmp) {
        KeyValueStore copyingKeyValueStore = new KeyValueStoreCopying(
                getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())),
                new KeyValueStoreLocalFileSystem(tmp, getKeyToPath(target),
                        new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())));

        KeyValueStore copyingKeyValueStoreIndex = new KeyValueStoreCopying(
                getKeyValueStore(new KeyValueStoreLocalFileSystem.KeyValueStreamFactoryValues(getHashType())),
                new KeyValueStoreLocalFileSystem(tmp, getKeyToPath(target),
                        new KeyValueStoreLocalFileSystem.KeyValueStreamFactoryValues(getHashType())));

        CloneUtil.clone(copyingKeyValueStore, copyingKeyValueStore, copyingKeyValueStoreIndex, getHashType());
    }

    private KeyToPath getKeyToPath(File target) {
        if (HashPathPattern.directoryDepth0.equals(pathPattern)) {
            return new KeyTo1LevelPath(target.toURI(), getHashType());
        } else {
            return new KeyTo3LevelPath(target.toURI(), getHashType());
        }
    }

    private void copyProvIndexOnly(File target, File tmp) {
        KeyValueStore copyingKeyValueStoreProv = new KeyValueStoreCopying(
                getKeyValueStore(new KeyValueStoreLocalFileSystem.KeyValueStreamFactoryValues(getHashType())),
                new KeyValueStoreLocalFileSystem(tmp, getKeyToPath(target),
                        new KeyValueStoreLocalFileSystem.KeyValueStreamFactoryValues(getHashType())));

        CloneUtil.clone(
                new NullKeyValueStore(),
                getKeyValueStore(new KeyValueStoreLocalFileSystem.KeyValueStreamFactoryValues(getHashType())),
                copyingKeyValueStoreProv, getHashType()
        );
    }

    private void copyProvLogsOnly(File target, File tmp) {
        KeyValueStore copyingKeyValueStoreProv = new KeyValueStoreCopying(
                getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())),
                new KeyValueStoreLocalFileSystem(tmp, getKeyToPath(target), new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())));

        CloneUtil.clone(
                new NullKeyValueStore(),
                copyingKeyValueStoreProv,
                getKeyValueStore(new KeyValueStoreLocalFileSystem.KeyValueStreamFactoryValues(getHashType())), getHashType()
        );
    }

    private void copyDataOnly(File target, File tmp) {
        KeyValueStore copyingKeyValueStoreBlob = new KeyValueStoreCopying(
                getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())),
                new KeyValueStoreLocalFileSystem(tmp,
                        getKeyToPath(target),
                        new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())));

        CloneUtil.clone(
                copyingKeyValueStoreBlob,
                getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())),
                getKeyValueStore(new KeyValueStoreLocalFileSystem.KeyValueStreamFactoryValues(getHashType())),
                getHashType());
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
