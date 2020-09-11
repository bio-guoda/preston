package bio.guoda.preston.process;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static bio.guoda.preston.model.RefNodeFactory.getVersion;
import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static bio.guoda.preston.process.ActivityUtil.emitAsNewActivity;

public class URLFinder extends ProcessorReadOnly {

    // From https://urlregex.com/
    static Pattern URL_PATTERN = Pattern.compile("(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]");

    public URLFinder(BlobStoreReadOnly blobStoreReadOnly, StatementsListener... listeners) {
        super(blobStoreReadOnly, listeners);
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)) {
            IRI version = (IRI) getVersion(statement);
            if (!attemptToParseAsZip(statement, version)) {
                attemptToParseAsText(statement, version);
            }
        }
    }
    private boolean attemptToParseAsZip(Quad statement, IRI version) {
        try (InputStream in = get(version)) {
            if (in != null) {
                List<Quad> nodes = new ArrayList<>();
                parseAsZip(version, in, new StatementsEmitterAdapter() {
                    @Override
                    public void emit(Quad statement) {
                        nodes.add(statement);
                    }
                });
                emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
                return true;
            }
        } catch (IOException e) {
            // ignore; this is opportunistic
        }
        return false;
    }

    private boolean attemptToParseAsText(Quad statement, IRI version) {
        try (InputStream in = get(version)) {
            if (in != null) {
                List<Quad> nodes = new ArrayList<>();
                parseAsText(version, in, new StatementsEmitterAdapter() {
                    @Override
                    public void emit(Quad statement) {
                        nodes.add(statement);
                    }
                });
                emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
                return true;
            }
        } catch (IOException e) {
            // ignore; this is opportunistic
        }
        return false;
    }

    private void parseAsZip(IRI version, InputStream in, StatementsEmitter emitter) throws IOException {
        ZipInputStream zIn = new ZipInputStream(in);

        ZipEntry entry;
        while ((entry = zIn.getNextEntry()) != null) {
            parseAsText(getEntryIri(version, entry.getName()), zIn, emitter);
        }
    }

    private void parseAsText(IRI version, InputStream in, StatementsEmitter emitter) {
        int line = 1;
        Scanner scanner = new Scanner(in);
        while (scanner.hasNextLine()) {
            String urlString;
            while ((urlString = scanner.findInLine(URL_PATTERN)) != null) {
                emitter.emit(toStatement(toIRI(urlString), toIRI("locatedAt"), getLineIri(version, line)));
            }
            scanner.nextLine();
            ++line;
        }
    }

    private static IRI getEntryIri(IRI version, String name) {
        return toIRI(String.format("zip:%s!/%s", version.getIRIString(), name));
    }

    private static IRI getLineIri(IRI fileIri, int line) {
        return toIRI(String.format("%s#L%d", fileIri.getIRIString(), line));
    }

}
