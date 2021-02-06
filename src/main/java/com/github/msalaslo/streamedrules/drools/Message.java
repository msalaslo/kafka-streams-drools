package com.github.msalaslo.streamedrules.drools;

/**
 * A simple object to store a message for processing by Drools.
 */
public class Message {
    private String content;

    public Message(String content) {
        this.content = content;
    }

    public String getContent() {
        return this.content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override public String toString() {
        return "Message{" + "content='" + content + '\'' + '}';
    }
}