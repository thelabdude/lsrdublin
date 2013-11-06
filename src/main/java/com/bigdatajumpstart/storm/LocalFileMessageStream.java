package com.bigdatajumpstart.storm;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

/**
 */
public class LocalFileMessageStream implements MessageStream {

    public String path;
    private FileReaderThread fileReaderThread;

    public void setPath(String path) {
        File verifyFile = new File(path);
        if (!verifyFile.isFile()) {
            throw new IllegalArgumentException("File "+path+"not found!");
        }
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    @Override
    public void receive(MessageHandler handler) {
        // must produce messages in the background
        fileReaderThread = new FileReaderThread(handler);
        fileReaderThread.start();
    }

    private class FileReaderThread extends Thread {
        MessageHandler handler;

        private FileReaderThread(MessageHandler handler) {
            this.handler = handler;
        }

        public void run() {
            String line;
            int lineNum = 0;
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
                while ((line = reader.readLine()) != null) {
                    ++lineNum;
                    line = line.trim();
                    if (line.length() > 0) {
                        handler.handleMessage(line);
                    }
                }
            } catch (Exception exc) {
                throw new RuntimeException(exc);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (Exception ignore){}
                }
            }
        }
    }
}
