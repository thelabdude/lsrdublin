package com.bigdatajumpstart.storm;

/**
 * Created with IntelliJ IDEA.
 * User: thelabdude
 * Date: 8/29/13
 * Time: 1:19 AM
 * To change this template use File | Settings | File Templates.
 */
public interface MessageHandler {
    void handleMessage(String msg);
}
