package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.Archiver;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.DereferencerContentAddressed;
import bio.guoda.preston.store.DereferencerCachingProxy;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.UUID;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.USED;
import static bio.guoda.preston.RefNodeConstants.WAS_GENERATED_BY;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toStatement;

@CommandLine.Command(
        name = "bash",
        description = "runs provided bash script and tracks stdout"
)
public class CmdBash extends CmdActivity {

    @CommandLine.Option(
            names = {"-c"},
            description = "Content id of script to be executed."
    )
    private IRI commandsContentId;

    private boolean streamingCommands = false;

    private File source = new File("/dev/stdin");

    @Override
    public String getDescriptionDefault() {
        return "Executes script and captures stdout";
    }

    @Override
    void initQueue(Queue<List<Quad>> statementQueue, ActivityContext ctx) {

    }

    @Override
    void processQueue(Queue<List<Quad>> statementQueue,
                      BlobStore blobStore,
                      ActivityContext ctx,
                      StatementsListener[] listeners) {
        StatementsListener processor = createActivityProcessor(blobStore, ctx, listeners);

        try {
            if (getCommandsContentId() == null) {
                setCommandsContentId(blobStore.put(getInputStream()));
                streamingCommands = true;
            }

            IRI output = RefNodeFactory.toIRI(UUID.randomUUID());
            statementQueue.add(Arrays.asList(
                    toStatement(getCommandsContentId(), RefNodeConstants.HAS_FORMAT, RefNodeFactory.toLiteral("text/x-shellscript")),
                    toStatement(ctx.getActivity(), USED, getCommandsContentId()),
                    toStatement(output, WAS_GENERATED_BY, ctx.getActivity()),
                    toStatement(output, HAS_VERSION, toBlank()))
            );

            while (!statementQueue.isEmpty()) {
                processor.on(statementQueue.poll());
            }
        } catch (IOException e) {
            //
        }
    }

    public void setCommandsContentId(IRI commandsContentId) {
        this.commandsContentId = commandsContentId;
    }

    private StatementsListener createActivityProcessor(
            BlobStore blobStore,
            ActivityContext ctx,
            StatementsListener[] listeners) {
        return new Archiver(
                new DereferencerCachingProxy(new DereferencerContentAddressed(uri -> {
                    InputStream inputStream = ContentQueryUtil.getContent(blobStore, getCommandsContentId(), this);
                    ProcessBuilder bash = new ProcessBuilder(
                            "bash",
                            "-c",
                            IOUtils.toString(inputStream, StandardCharsets.UTF_8)
                    );

                    Process proc = streamingCommands
                            ? bash.start()
                            : bash.redirectInput(ProcessBuilder.Redirect.from(source)).start();
                    return proc.getInputStream();
                }, blobStore)),
                ctx,
                listeners);
    }


    public IRI getCommandsContentId() {
        return commandsContentId;
    }

    public File getSource() {
        return source;
    }

    public void setSource(File source) {
        this.source = source;
    }

}
