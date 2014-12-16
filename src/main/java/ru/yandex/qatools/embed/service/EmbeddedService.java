package ru.yandex.qatools.embed.service;

/**
 * @author smecsia
 */
public interface EmbeddedService {
    void start();

    void stop();

    boolean isStarted();
}
