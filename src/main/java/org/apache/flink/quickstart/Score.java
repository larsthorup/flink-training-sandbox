package org.apache.flink.quickstart;

public class Score {
    public String user;
    public String subject;
    public float value;
    public Score() {}
    public Score(String user, String subject, float value) {
        this.user = user;
        this.subject = subject;
        this.value = value;
    }
    @Override
    public String toString() {
        return "Score(" + user + ", " + subject + ": " + value + ")";
    }
};
