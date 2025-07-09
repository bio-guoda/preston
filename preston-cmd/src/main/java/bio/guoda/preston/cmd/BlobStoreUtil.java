package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.EmittingStreamFactory;
import bio.guoda.preston.process.EmittingStreamOfAnyQuad;
import bio.guoda.preston.process.ParsingEmitter;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.store.AliasDereferencer;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.HashKeyUtil;
import bio.guoda.preston.stream.ContentHashDereferencer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.globalbioticinteractions.doi.DOI;
import org.globalbioticinteractions.doi.MalformedDOIException;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class BlobStoreUtil {
    private static final Logger LOG = LoggerFactory.getLogger(BlobStoreUtil.class);
    public static final List<IRI> ALTERNATE_VERBS = Arrays.asList(RefNodeConstants.ALTERNATE_OF, RefNodeConstants.SEE_ALSO);

    public static BlobStoreReadOnly createIndexedBlobStoreFor(BlobStoreReadOnly blobStoreReadOnly,
                                                              Dereferencer<IRI> versionForAliasLookup) {
        return new BlobStoreReadOnly() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                IRI iriForLookup = versionForAliasLookup.get(uri);

                if (iriForLookup == null) {
                    throw new IOException("failed to find content associated to [" + uri + "] in index.");
                }
                return blobStoreReadOnly.get(iriForLookup);
            }

        };
    }

    public static Dereferencer<IRI> doiForContent(Persisting persisting) {
        File tmpDir = new File(persisting.getTmpDir());
        IRI provenanceAnchor = AnchorUtil.findAnchorOrThrow(persisting);

        // indexing
        DB db = getFileDb(tmpDir);

        Map<String, String> versionMap = db
                .createTreeMap("versionMap")
                .make();

        Map<String, String> alternateMap = db
                .createTreeMap("alternateMap")
                .make();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        AtomicLong index = new AtomicLong(0);
        LOG.info("doi index for [" + provenanceAnchor + "] building...");

        StatementsListener listener = new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                if (hasIRIs(statement)) {
                    IRI object = (IRI) statement.getObject();
                    IRI subject = (IRI) statement.getSubject();
                    if (RefNodeConstants.HAS_VERSION.equals(statement.getPredicate())
                            && !RefNodeFactory.isBlankOrSkolemizedBlank(statement.getObject())) {
                        indexVersion(statement, object);
                    } else if (ALTERNATE_VERBS.contains(statement.getPredicate())) {
                        indexAlternate(object, subject);
                    }
                }
            }

            void indexAlternate(IRI object, IRI subject) {
                alternateMap.putIfAbsent(object.getIRIString(), subject.getIRIString());
                alternateMap.putIfAbsent(subject.getIRIString(), object.getIRIString());
            }

            void indexVersion(Quad statement, IRI version) {
                if (HashKeyUtil.isValidHashKey(version)) {
                    index.incrementAndGet();
                    String uri = ((IRI) statement.getSubject()).getIRIString();
                    String indexedVersion = version.getIRIString();
                    versionMap.putIfAbsent(indexedVersion, uri);
                }
            }

            public boolean hasIRIs(Quad statement) {
                return statement.getSubject() instanceof IRI && statement.getObject() instanceof IRI;
            }
        };
        ReplayUtil.replay(listener, persisting, new EmittingStreamFactory() {
            @Override
            public ParsingEmitter createEmitter(StatementEmitter emitter, ProcessorState context) {
                return new EmittingStreamOfAnyQuad(emitter, context);
            }
        });
        stopWatch.stop();
        LOG.info("doi index for [" + provenanceAnchor + "] with [" + index.get() + "] versions built in [" + stopWatch.getTime(TimeUnit.SECONDS) + "] s");

        return new ContentReferencer(alternateMap, versionMap);
    }

    public static Dereferencer<IRI> contentForAlias(Persisting persisting) {
        Pair<Map<String, String>, Map<String, String>> aliasAndVersionMaps = buildIndexedBlobStore(persisting);
        Map<String, String> versionMap = aliasAndVersionMaps.getKey();
        Map<String, String> aliasMap = aliasAndVersionMaps.getValue();

        return new IRIDereferencer(aliasMap, versionMap);
    }

    public static BlobStoreReadOnly createResolvingBlobStoreFor(Dereferencer<InputStream> blobStore, Persisting persisting) {
        return new AliasDereferencer(
                new ContentHashDereferencer(blobStore),
                persisting,
                persisting.getProvenanceTracer()
        );
    }


    private static org.apache.commons.lang3.tuple.Pair<Map<String, String>, Map<String, String>> buildIndexedBlobStore(Persisting persisting) {

        File tmpDir = new File(persisting.getTmpDir());
        IRI provenanceAnchor = AnchorUtil.findAnchorOrThrow(persisting);

        // indexing
        DB db = getFileDb(tmpDir);

        Map<String, String> versionMap = db
                .createTreeMap("versionMap")
                .make();

        Map<String, String> alternateMap = db
                .createTreeMap("alternateMap")
                .make();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        AtomicLong index = new AtomicLong(0);
        LOG.info("version index for [" + provenanceAnchor + "] building...");

        StatementsListener listener = new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                if (hasIRIs(statement)) {
                    IRI object = (IRI) statement.getObject();
                    IRI subject = (IRI) statement.getSubject();
                    if (RefNodeConstants.HAS_VERSION.equals(statement.getPredicate())
                            && !RefNodeFactory.isBlankOrSkolemizedBlank(statement.getObject())) {
                        indexVersion(statement, object);
                    } else if (ALTERNATE_VERBS.contains(statement.getPredicate())) {
                        indexAlternate(object, subject);
                    }
                }
            }

            void indexAlternate(IRI object, IRI subject) {
                alternateMap.putIfAbsent(object.getIRIString(), subject.getIRIString());
                alternateMap.putIfAbsent(subject.getIRIString(), object.getIRIString());
            }

            void indexVersion(Quad statement, IRI object) {
                IRI version = object;
                if (HashKeyUtil.isValidHashKey(version)) {
                    index.incrementAndGet();
                    String uri = ((IRI) statement.getSubject()).getIRIString();
                    String indexedVersion = version.getIRIString();
                    versionMap.putIfAbsent(uri, indexedVersion);
                }
            }

            public boolean hasIRIs(Quad statement) {
                return statement.getSubject() instanceof IRI && statement.getObject() instanceof IRI;
            }
        };
        ReplayUtil.replay(listener, persisting, new EmittingStreamFactory() {
            @Override
            public ParsingEmitter createEmitter(StatementEmitter emitter, ProcessorState context) {
                return new EmittingStreamOfAnyQuad(emitter, context);
            }
        });
        stopWatch.stop();
        LOG.info("version index for [" + provenanceAnchor + "] with [" + index.get() + "] versions built in [" + stopWatch.getTime(TimeUnit.SECONDS) + "] s");

        return org.apache.commons.lang3.tuple.Pair.of(versionMap, alternateMap);
    }

    public static DB getFileDb(File tmpDir) {
        DB db = newTmpFileDB(tmpDir)
                .deleteFilesAfterClose()
                .closeOnJvmShutdown()
                .transactionDisable()
                .make();
        return db;
    }

    private static DBMaker newTmpFileDB(File tmpDir) {
        try {
            File db = File.createTempFile("mapdb-temp", "db", tmpDir);
            return DBMaker.newFileDB(db);
        } catch (IOException e) {
            throw new IOError(new IOException("failed to create tmpFile in [" + tmpDir.getAbsolutePath() + "]", e));
        }

    }

    private static class IRIDereferencer implements Dereferencer<IRI> {

        private final Map<String, String> aliasMap;
        private final Map<String, String> versionMap;

        public IRIDereferencer(Map<String, String> aliasMap, Map<String, String> versionMap) {
            this.aliasMap = aliasMap;
            this.versionMap = versionMap;
        }

        @Override
        public IRI get(IRI uri) throws IOException {
            IRI iriForLookup;
            if (HashKeyUtil.isValidHashKey(uri)) {
                iriForLookup = uri;
            } else {
                String redirectCursor = uri.getIRIString();
                List<String> redirects = new ArrayList<>();
                redirects.add(redirectCursor);
                String redirectCandidate;
                while ((redirectCandidate = aliasMap.get(redirectCursor)) != null) {
                    if (StringUtils.isNotBlank(redirectCandidate)
                            && !redirects.contains(redirectCandidate)) {
                        redirects.add(redirectCandidate);
                        redirectCursor = redirectCandidate;
                    } else {
                        break;
                    }
                }
                String indexedVersion = StringUtils.isBlank(redirectCursor)
                        ? versionMap.get(uri.getIRIString())
                        : versionMap.get(redirectCursor);

                iriForLookup = StringUtils.isBlank(indexedVersion)
                        ? null
                        : RefNodeFactory.toIRI(indexedVersion);
            }
            return iriForLookup;
        }
    }

    private static class ContentReferencer implements Dereferencer<IRI> {

        private final Map<String, String> aliasMap;
        private final Map<String, String> versionMap;

        public ContentReferencer(Map<String, String> aliasMap, Map<String, String> versionMap) {
            this.aliasMap = aliasMap;
            this.versionMap = versionMap;
        }

        @Override
        public IRI get(IRI contentId) throws IOException {
            String doiCandidate = null;
            String redirectCursor = versionMap.get(contentId.getIRIString());
            if (StringUtils.isNotBlank(redirectCursor)) {
                List<String> redirects = new ArrayList<>();
                redirects.add(redirectCursor);
                String redirectCandidate;
                while ((redirectCandidate = aliasMap.get(redirectCursor)) != null) {
                    if (StringUtils.isNotBlank(redirectCandidate)
                            && !redirects.contains(redirectCandidate)) {
                        redirects.add(redirectCandidate);
                        redirectCursor = redirectCandidate;
                        try {
                            DOI.create(redirectCursor);
                            doiCandidate = redirectCursor;
                        } catch (MalformedDOIException ex) {
                            // ignore
                        }
                    } else {
                        break;
                    }
                }
            }
            return StringUtils.isBlank(doiCandidate)
                    ? null
                    : RefNodeFactory.toIRI(doiCandidate);
        }
    }
}
