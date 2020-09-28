import entity.Job;
import org.apache.log4j.helpers.OptionConverter;

import java.io.IOException;
import java.util.*;

public class CheckJob implements Runnable {
    private Runtime Process = Runtime.getRuntime();
    private List<Job> runjobs = new ArrayList<Job>();
    private Map<String, String> jobInfo = new HashMap<String, String>();

    @Override
    public void run() {
        while (true) {
            try {
                runjobs = runningJob(Runner.hc.getMethod(Runner.hc.getProp().getProperty("flink.rest.url") + "/jobs/overview"));
                if (runjobs.size() != 0) {
                    jobInfo.clear();
                    for (Job n : runjobs
                            ) {
                        jobInfo.put(n.getName(), n.getJid());
                    }
                }
                /*第一次启动时，注入已经运行作业*/
                if (Runner.index.size() == 0 && jobInfo.size() != 0) {
                    Runner.index.putAll(jobInfo);
                }
                /*判断运行状态*/
                if (Runner.index.size() > runjobs.size()) {
                    synchronized (Runner.o) {
                        for (Iterator<String> it = Runner.index.keySet().iterator(); it.hasNext(); ) {
                            String item = it.next();
                            if ((!jobInfo.keySet().contains(item)) && (Runner.index.get(item).equals(""))) {
                                startJob(item.replace("数据交换平台智能路由-", ""));
                                Runner.logger.info("Started new processor for cluster {},please check it", item.replace("数据交换平台智能路由-", ""));
                            }
                        }
                    }
                } else if (Runner.index.size() < runjobs.size()) {
                    synchronized (Runner.o) {
                        for (Iterator<String> it = jobInfo.values().iterator(); it.hasNext(); ) {
                            String item = it.next();
                            if (!Runner.index.values().contains(item)) {
                                cancelJob(item);
                                Runner.logger.info("Find unnecessary processor, and stopped it");
                            }
                        }
                    }
                } else if (Runner.index.values().contains("sql updated")) {
                    synchronized (Runner.o) {
                        for (Iterator<String> it = Runner.index.keySet().iterator(); it.hasNext(); ) {
                            String item = it.next();
                            if (!Runner.index.get(item).equals("")) {
                                cancelJob(jobInfo.get(item));
                                startJob(item.replace("数据交换平台智能路由-", ""));
                                Runner.logger.info("Restart processor for cluster {},please check it", item.replace("数据交换平台智能路由-", ""));
                            }
                        }
                    }
                }
                Thread.sleep(OptionConverter.toInt(Runner.hc.getProp().getProperty("check.timespan"), 5) * 1000);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private void cancelJob(String jobid) throws InterruptedException, IOException {
        synchronized (Runner.o) {
            String command1 = String.format("/opt/flink/bin/flink cancel %s", jobid);
            Process.exec(command1);
            Thread.sleep(1000);//等待作业完成取消操作
        }
    }

    private void startJob(String clusterName) throws InterruptedException, IOException {
        String command = String.format("/opt/flink/bin/flink run /opt/flink/examples/streaming/%s --apollo.cluster %s", Runner.hc.getProp().getProperty("jobname"), clusterName);
        Process.exec(command);
        Thread.sleep(1000);
        //logger.info("Create %s job",clusterName);
        ansis();
    }

    /*判断新建作业是否已经提交，若没有则递归*/
    private void ansis() throws InterruptedException {
        try {
            runjobs = runningJob(Runner.hc.getMethod(Runner.hc.getProp().getProperty("flink.rest.url") + "/jobs/overview"));
            if (Runner.index.size() != runjobs.size()) {
                Contrast(runjobs);
            } else {
                /*作业提交需要时间，等待作业提交完毕*/
                Thread.sleep(1 * 1000);
                ansis();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*添加新增作业的信息*/
    private void Contrast(List<Job> jobs) {
        if (jobs.size() != 0) {
            for (Job n : jobs) {
                synchronized (Runner.o) {
                    if (!Runner.index.keySet().contains(n.getName())) {
                        Runner.index.put(n.getName(), n.getJid());
                    }
                }
            }
        }
    }

    private List<Job> runningJob(List<Job> jobs) {
        List<Job> runs = new ArrayList<Job>();
        for (Job n : jobs) {
            if (n.getState().equals("RUNNING")) {
                runs.add(n);
            }
        }
        return runs;
    }

}
