import Common.jobId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConfigListener implements Runnable {
    private String clusterName;
    private List<jobId> jobs = new ArrayList<jobId>();


    public ConfigListener(String _clustername, List<jobId> _jobs) {
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

                ansis();
                //String value=change.getNewValue()+','+jobid;
                //change.setNewValue(value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Started new Job for cluster {},please checkout it", clusterName);
    }

    /*判断新建作业是否已经提交，若没有则递归*/
    private void ansis() {
        try {
            if (jobs.size() != Runner.hc.getMethod("http://192.168.191.131:8082/jobs").size()) {
                jobs = Runner.hc.getMethod("http://192.168.191.131:8082/jobs");
                Contrast(jobs);
            } else {
                /*作业提交需要时间，等待作业提交完毕*/
                Thread.sleep(5 * 1000);
                ansis();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void Contrast(List<jobId> jobs) {
        if (jobs.size() != 0) {
            for (jobId n : jobs) {
                int num = 0;
                for (String jobid : Runner.index.values()) {
                    if (jobid.equals(n.getId()))
                        break;
                    else {
                        num++;
                        if (num == Runner.index.values().size()) {
                            if (!n.getStatus().equals("CANCELED"))
                                Runner.index.put(clusterName, n.getId());
                        }
                    }
                }
            }
        }
    }
}
