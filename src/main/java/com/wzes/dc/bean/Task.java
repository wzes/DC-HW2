package com.wzes.dc.bean;

/**
 * @author Create by xuantang
 * @date on 11/8/17
 */
public class Task {
    private Long start;
    private int length;

    public Task(Long start, int length) {
        this.start = start;
        this.length = length;
    }

    public Task() {
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }
}
