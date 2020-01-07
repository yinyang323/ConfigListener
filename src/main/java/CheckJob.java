import Common.jobId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CheckJob implements Runnable {
    @Override
    public void run() {
        while(true){
            try {
                List<String> runjobs=runningJob(Runner.hc.getMethod(Runner.hc.getProp().getProperty("flink.rest.url") + "/jobs"));
                if (Runner.index.size() != runjobs.size()) {
                    for ( String n:Runner.index.keySet()
                         ) {
                        if (!runjobs.contains(Runner.index.get(n))) {
                            synchronized (Runner.o) {
                                Runner.index.remove(n);
                            }
                        }
                    }
                } else {
                    /*作业提交需要时间，等待作业提交完毕*/
                    Thread.sleep(5 * 1000);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    /*获取所有有效作业的jobid*/
    private List<String> runningJob(List<jobId> jobs){
        List<String> runs = new ArrayList<String>();
        for (jobId n:jobs
                ) {
            if(n.getStatus().equals("RUNNING")){
                runs.add(n.getId());
            }
        }
        return runs;
    }

}
