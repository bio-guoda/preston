package bio.guoda.preston.cmd;

import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.process.TextReader;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static java.lang.System.exit;

@Parameters(separators = "= ", commandDescription = "get biodiversity data")
public class CmdGet extends Persisting implements Runnable {

    @Parameter(description = "data content-hash uri (e.g., [hash://sha256/8ed311...])",
            validateWith = URIValidator.class)
    private List<String> contentUris = new ArrayList<>();

    @Override
    public void run() {
        BlobStoreReadOnly blobStore = new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()));
        run(blobStore);
    }

    public void run(BlobStoreReadOnly blobStore) {
        try {
            if (contentUris.isEmpty()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                String line;
                while ((line = reader.readLine()) != null) {
                    handleContentQuery(blobStore, StringUtils.trim(line));
                }
            } else {
                for (String s : contentUris) {
                    handleContentQuery(blobStore, s);
                }
            }
        } catch (Throwable th) {
            th.printStackTrace(System.err);
            exit(1);
        }
    }

    protected void handleContentQuery(BlobStoreReadOnly blobStore, String queryString) throws IOException {
        ContentDereferencer query = new ContentDereferencer(blobStore);

        try {
            InputStream contentStream = query.dereference(toIRI(queryString));
            IOUtils.copyLarge(contentStream, System.out);
        } catch (IOException e) {
            throw new IOException("problem retrieving [" + queryString + "]", e);
        }
    }

    public void setContentUris(List<String> contentUris) {
        this.contentUris = contentUris;
    }

    private class ContentDereferencer extends TextReader implements Dereferencer<InputStream> {

        private final BlobStoreReadOnly blobStore;

        private String targetIriString;
        private IRI contentHash;
        private InputStream contentStream;

        public ContentDereferencer(BlobStoreReadOnly blobStore) {
            this.blobStore = blobStore;
        }

        @Override
        public void attemptToParse(IRI version, InputStream in) throws IOException, URISyntaxException {
            Matcher nextOperatorMatcher = Pattern.compile(String.format("([^:]+):%s", version.getIRIString())).matcher(targetIriString);

            if (version.getIRIString().equals(targetIriString)) {
                contentStream = in;
                stopReading();
            }
            else if (nextOperatorMatcher.find() && nextOperatorMatcher.group(1).equals("cut")) {
                cutAndParseBytes(version, in);
            }
            else {
                super.attemptToParse(version, in);
            }
        }

        private void cutAndParseBytes(IRI version, InputStream in) throws IOException, URISyntaxException {
            // do not support open-ended cuts, e.g. "b5-" or "b-5"
            Matcher byteRangeMatcher = Pattern.compile(String.format("^cut:%s!/b(?<first>[0-9]+)-(?<last>[0-9]+)$", version.getIRIString())).matcher(targetIriString);
            if (byteRangeMatcher.find()) {
                long firstByteIndex = Long.parseLong(byteRangeMatcher.group("first")) - 1;
                long lastByteIndex = Long.parseLong(byteRangeMatcher.group("last"));

                attemptToParse(
                        toIRI(byteRangeMatcher.group()),
                        cutBytes(in, firstByteIndex, lastByteIndex)
                );
            }
            else {
                throw new IllegalArgumentException();
            }
        }

        private InputStream cutBytes(InputStream in, long firstByteIndex, long lastByteIndex) throws IOException {
            IOUtils.skipFully(in, firstByteIndex);
            return new BoundedInputStream(in, (lastByteIndex - firstByteIndex));
        }

        @Override
        protected boolean shouldReadArchiveEntry(IRI entryIri) {
            return isPartOfTargetIri(entryIri);
        }

        private boolean isPartOfTargetIri(IRI version) {
            return targetIriString.contains(version.getIRIString());
        }

        @Override
        public InputStream dereference(IRI uri) throws IOException {
            targetIriString = uri.getIRIString();

            final Pattern contentHashPattern = Pattern.compile("hash://sha256/[a-fA-F0-9]{64}");
            Matcher matchHash = contentHashPattern.matcher(targetIriString);
            if (matchHash.find()) {
                contentHash = toIRI(matchHash.group());
            }

            contentStream = null;

            InputStream is = blobStore.get(contentHash);
            if (is != null) {
                try {
                    attemptToParse(contentHash, is);
                } catch (URISyntaxException ignored) {
                }
            }

            if (contentStream == null) {
                throw new IOException("failed to resolve [\" + contentHash.getIRIString() + \"]");
            }
            else {
                return contentStream;
            }
        }
    }

}
