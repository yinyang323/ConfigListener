import entity.Job;
import org.apache.log4j.helpers.OptionConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CheckJob implements Runnable {
    @Override
    public void run() {
        while (true) {
            try {
                List<String> runjobs = runningJob(Runner.hc.getMethod(Runner.hc.getProp().getProperty("flink.rest.url") + "/jobs/overview"));
                /*第一次启动时，注入已经运行作业*/
                if (Runner.index.size() == 0) {
                    Runner.index = runjobs;
                }
                if (Runner.index.size() != runjobs.size()) {
                    synchronized (Runner.o) {
                        for (Iterator<String> it = Runner.index.iterator(); it.hasNext(); ) {
                            String item = it.next();
                            if (!runjobs.contains(item))
                                it.remove();
                        }
                    }
                } else {
                    /*作业提交需要时间，等待作业提交完毕*/
                    Thread.sleep(OptionConverter.toInt(Runner.hc.getProp().getProperty("check.timespan"), 5) * 1000);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    /*获取所有有效作业的Job*/
    private List<String> runningJob(List<Job> jobs) {
        List<String> runs = new ArrayList<String>();
        for (Job n : jobs) {
            if (n.getState().equals("RUNNING")) {
                runs.add(n.getName());
            }
        }
        return runs;
    }

}
