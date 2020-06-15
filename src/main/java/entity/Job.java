/**
 * Copyright 2020 bejson.com
 */
package entity;

/**
 * Auto_generated: 2020_06_12 11:6:11
 *
 * @author bejson.com (i@bejson.com)
 * @website http://www.bejson.com/java2pojo/
 */
public class Job {

    private String jid;
    private String name;
    private String state;
    private long start_time;
    private long end_time;
    private long duration;
    private long last_modification;
    private Tasks tasks;
    public void setJid(String jid) {
        this.jid = jid;
    }
    public String getJid() {
        return jid;
    }

    public void setName(String name) {
        this.name = name;
    }
    public String getName() {
        return name;
    }

    public void setState(String state) {
        this.state = state;
    }
    public String getState() {
        return state;
    }

    public void setStart_time(long start_time) {
        this.start_time = start_time;
    }
    public long getStart_time() {
        return start_time;
    }

    public void setEnd_time(long end_time) {
        this.end_time = end_time;
    }
    public long getEnd_time() {
        return end_time;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }
    public long getDuration() {
        return duration;
    }

    public void setLast_modification(long last_modification) {
        this.last_modification = last_modification;
    }
    public long getLast_modification() {
        return last_modification;
    }

    public void setTasks(Tasks tasks) {
        this.tasks = tasks;
    }
    public Tasks getTasks() {
        return tasks;
    }

}