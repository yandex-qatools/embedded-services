package ru.yandex.qatools.embed.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static jodd.io.FileUtil.createTempDirectory;
import static jodd.io.FileUtil.deleteDir;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Ilya Sadykov
 */
public abstract class AbstractEmbeddedService implements EmbeddedService {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final String dataDirectory;
    protected final int initTimeout;
    protected final boolean removeDataDir;
    protected final boolean enabled;
    protected final boolean newDirectory;
    protected volatile boolean stopped = false;
    protected volatile boolean started = false;

    public AbstractEmbeddedService(String dataDirectory, boolean enabled, int initTimeout) throws IOException {
        this.enabled = enabled;
        this.initTimeout = initTimeout;

        if (isEmpty(dataDirectory) || dataDirectory.equals("TMP")) {
            this.removeDataDir = true;
            this.dataDirectory = createTempDirectory(getClass().getSimpleName().toLowerCase(), "data").getPath();
            this.newDirectory = true;
        } else {
            this.dataDirectory = dataDirectory;
            this.removeDataDir = false;
            this.newDirectory = !new File(dataDirectory).exists();
        }
    }


    protected abstract void doStart() throws Exception;

    protected abstract void doStop() throws Exception;

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public void start() {
        if (this.enabled) {
            try {
                logger.info("Starting the embedded service...");
                doStart();
                started = true;
            } catch (Exception e) {
                logger.error("Failed to start embedded service", e);
            }
        }
    }

    @Override
    public void stop() {
        if (!stopped) {
            logger.info("Shutting down the embedded service...");
            started = false;
            try {
                doStop();
                stopped = true;
                if (removeDataDir) {
                    try {
                        deleteDir(new File(dataDirectory));
                    } catch (Exception e) {
                        logger.error("Failed to remove data dir", e);
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to stop service", e);
            }
        }

    }
}
