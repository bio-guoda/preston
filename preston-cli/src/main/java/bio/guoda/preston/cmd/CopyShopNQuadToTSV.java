package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorStateReadOnly;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CopyShopNQuadToTSV implements CopyShop {

    public static final String SUBJECT_VERB_IRI_OBJECT_NAMESPACE
            = "<(?<subject>.*)> <(?<verb>.*)> <(?<object>.*)>[ ]<(?<namespace>.*)> \\.";

    public static final String SUBJECT_VERB_LITERAL_OBJECT_NAMESPACE
            = "<(?<subject>.*)> <(?<verb>.*)>[ ](?<object>.*)[ ]<(?<namespace>.*)> \\.";

    public static final Pattern WITH_IRI_OBJECT
            = Pattern.compile(SUBJECT_VERB_IRI_OBJECT_NAMESPACE);

    public static final Pattern WITH_LITERAL_OBJECT
            = Pattern.compile(SUBJECT_VERB_LITERAL_OBJECT_NAMESPACE);

    private final ProcessorStateReadOnly context;

    public CopyShopNQuadToTSV(ProcessorStateReadOnly context) {
        this.context = context;
    }

    @Override
    public void copy(InputStream is, OutputStream os) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        String line;
        while (getContext().shouldKeepProcessing() && (line = reader.readLine()) != null) {
            Matcher matcher = WITH_IRI_OBJECT.matcher(line);
            if (!matcher.matches()) {
                matcher = WITH_LITERAL_OBJECT.matcher(line);
            }

            if (matcher.matches()) {
                Stream<String> groups = Stream.of(matcher.group("subject"), matcher.group("verb"), matcher.group("object"), matcher.group("namespace"));
                IOUtils.write(groups.collect(Collectors.joining("\t", "", "\n")), os, StandardCharsets.UTF_8);
            }
        }
    }

    public ProcessorStateReadOnly getContext() {
        return context;
    }
}
