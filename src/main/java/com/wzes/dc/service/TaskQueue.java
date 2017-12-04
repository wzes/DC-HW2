package com.wzes.dc.service;

import com.wzes.dc.bean.Task;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Create by xuantang
 * @date on 11/8/17
 */
public class TaskQueue {

    private static TaskQueue taskQueue;

    public boolean isProduceEnd() {
        return produceEnd;
    }

    private static boolean produceEnd = false;

    public Queue<Task> getQueue() {
        return queue;
    }

    private Queue<Task> queue;

    private TaskQueue() {
        queue = new LinkedList<Task>();
    }

    public static synchronized TaskQueue getInstance() {
        if(null == taskQueue) {
            taskQueue = new TaskQueue();
        }
        return taskQueue;
    }

    public void addTask(Task task) {
        queue.add(task);
    }
    public boolean isQueueEmpty() {
        return queue.isEmpty();
    }
    public synchronized Task getTask() {
        if(!queue.isEmpty()) {
            return queue.poll();
        }
        return null;
    }

    public void setProduceEnd(boolean produceEnd) {
        TaskQueue.produceEnd = produceEnd;
    }


}
