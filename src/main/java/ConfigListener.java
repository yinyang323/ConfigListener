import entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConfigListener implements Runnable {
    private String clusterName;

    public ConfigListener(String _clustername) {
        clusterName = _clustername;
    }

    @Override
    public void run() {
        final Logger logger = LoggerFactory.getLogger(ConfigListener.class);

        Runtime Process = Runtime.getRuntime();

        //String Job = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 30);
        try {
            if (!Runner.index.contains("数据交换平台智能路由" + clusterName)) {
                String command = String.format("./flink run ../examples/streaming/my-flink-project0.3.jar --apollo.cluster %s", clusterName);
                Process.exec(command);
                //logger.info("Create %s job",clusterName);

                ansis();
                //String value=change.getNewValue()+','+Job;
                //change.setNewValue(value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } /*catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        logger.info("Started new Job for cluster {},please checkout it", clusterName);
    }

    /*判断新建作业是否已经提交，若没有则递归*/
    private void ansis() throws InterruptedException {
        try {
            List<Job> runjobs = runningJob(Runner.hc.getMethod(Runner.hc.getProp().getProperty("flink.rest.url") + "/jobs/overview"));
            if (Runner.index.size() < runjobs.size()) {
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

    private void Contrast(List<Job> jobs) {
        if (jobs.size() != 0) {
            for (Job n : jobs) {
                synchronized (Runner.o) {
                    if (!Runner.index.contains(n.getName())) {
                        Runner.index.add(n.getName());
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
