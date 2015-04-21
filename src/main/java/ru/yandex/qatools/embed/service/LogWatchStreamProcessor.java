package ru.yandex.qatools.embed.service;

import de.flapdoodle.embed.process.io.IStreamProcessor;

import java.util.Set;

/**
 * @author Ilya Sadykov
 */
public class LogWatchStreamProcessor extends de.flapdoodle.embed.process.io.LogWatchStreamProcessor {
    private final Object mutex = new Object();
    private final String success;
    private volatile boolean found = false;

    public LogWatchStreamProcessor(String success, Set<String> failures, IStreamProcessor destination) {
        super(success, failures, destination);
        this.success = success;
    }

    @Override
    public void process(String block) {
        if (block.contains(success)) {
            synchronized (mutex) {
                found = true;
                mutex.notifyAll();
            }
        } else {
            super.process(block);
        }
    }

    public void waitForResult(long timeout) {
        synchronized (mutex) {
            try {
                while (!found) {
                    mutex.wait(timeout);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
