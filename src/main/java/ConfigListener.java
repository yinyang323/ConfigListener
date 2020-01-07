import Common.jobId;
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

        //String jobid = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 30);
        try {
            if (!Runner.index.containsKey(clusterName)) {
                String command = String.format("./flink run ../examples/streaming/my-flink-project0.3.jar --apollo.cluster %s", clusterName);
                Process.exec(command);
                //logger.info("Create %s job",clusterName);

                ansis();
                //String value=change.getNewValue()+','+jobid;
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
            List<jobId> runjobs=runningJob(Runner.hc.getMethod(Runner.hc.getProp().getProperty("flink.rest.url") + "/jobs"));
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

    private void Contrast(List<jobId> jobs) {
        if (jobs.size() != 0) {
            for (jobId n : jobs) {
                if (!Runner.index.values().contains(n.getId())) {
                    synchronized (Runner.o) {
                        Runner.index.put(clusterName, n.getId());
                    }
                }
            }
        }
    }

    private List<jobId> runningJob(List<jobId> jobs){
        List<jobId> runs = new ArrayList<jobId>();
        for (jobId n:jobs
             ) {
            if(n.getStatus().equals("RUNNING")){
                runs.add(n);
            }
        }
        return runs;
    }
}
