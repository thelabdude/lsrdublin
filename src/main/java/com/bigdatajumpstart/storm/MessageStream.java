package com.bigdatajumpstart.storm;

/**
 * Interface to a source that provides a stream of messages to
 * the specified MessageHandler.
 */
public interface MessageStream {
    void receive(MessageHandler handler);
}
