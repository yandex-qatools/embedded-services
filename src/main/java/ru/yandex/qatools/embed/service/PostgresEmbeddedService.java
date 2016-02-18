package ru.yandex.qatools.embed.service;

import de.flapdoodle.embed.process.config.IRuntimeConfig;
import ru.yandex.qatools.embed.postgresql.Command;
import ru.yandex.qatools.embed.postgresql.PostgresExecutable;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.DownloadConfigBuilder;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;
import ru.yandex.qatools.embed.postgresql.config.RuntimeConfigBuilder;
import ru.yandex.qatools.embed.postgresql.ext.ArtifactStoreBuilder;

import java.io.IOException;

import static ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig.*;
import static ru.yandex.qatools.embed.postgresql.distribution.Version.Main.PRODUCTION;

/**
 * Embedded Postgres server
 */
public class PostgresEmbeddedService extends AbstractEmbeddedService {

    protected final String host;
    protected final int port;
    protected final String dbName;
    protected final String username;
    protected final String password;
    PostgresProcess process;

    public PostgresEmbeddedService(String host, int port, String username, String password, String dbName) throws IOException {
        this(host, port, username, password, dbName, null, true, 10000);
    }

    public PostgresEmbeddedService(String host, int port,
                                   String username, String password, String dbName,
                                   String dataDirectory, boolean enabled, int initTimeout) throws IOException {
        super(dataDirectory, enabled, initTimeout);
        this.username = username;
        this.password = password;
        this.host = host;
        this.port = port;
        this.dbName = dbName;
    }

    @Override
    public void doStart() throws Exception {
        final IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
                .defaults(Command.Postgres)
                .artifactStore(new ArtifactStoreBuilder()
                        .defaults(Command.Postgres)
                        .download(new DownloadConfigBuilder()
                                .defaultsForCommand(Command.Postgres)
                                .build()
                        )
                ).build();
        final PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getInstance(runtimeConfig);
        final PostgresConfig configDb = new PostgresConfig(
                PRODUCTION, new Net(host, port), new Storage(dbName, dataDirectory),
                new Timeout(initTimeout), new Credentials(username, password));
        PostgresExecutable exec = runtime.prepare(configDb);
        process = exec.start();
    }

    @Override
    public void doStop() throws Exception {
        if (process != null) {
            process.stop();
            process = null;
        }
    }
}
