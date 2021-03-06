import Common.ApolloOpen;
import Common.httpClient;
import com.ctrip.framework.apollo.openapi.dto.OpenEnvClusterDTO;
import com.ctrip.framework.apollo.openapi.dto.OpenReleaseDTO;
import org.apache.log4j.helpers.OptionConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

import java.sql.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Runner {
    //static private Connection conn = null;
    //static private Statement stmt = null;
    static public List<String> index = new ArrayList<String>();
    static private List<String> rslist = new ArrayList<String>();
    static public httpClient hc = null;
    static private ApolloOpen apollo;
    static private String appid;
    static private String env;
    static public final Object o = new Object();
    static Logger logger;

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        logger = LoggerFactory.getLogger(Runner.class);
        hc = new httpClient();
        CheckJob cj = new CheckJob();

        Class.forName("com.mysql.cj.jdbc.Driver");

        appid = hc.getProp().getProperty("app.id");
        env = hc.getProp().getProperty("env");
        apollo = new ApolloOpen(hc.getProp().getProperty("apollo.portalUrl"), hc.getProp().getProperty("apollo.token"));

        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 10, 200,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(5));

        executor.execute(cj);
        logger.info("线程池中线程数目：" + executor.getPoolSize() + "，队列中等待执行的任务数目：" +
                executor.getQueue().size() + "，已执行玩别的任务数目：" + executor.getCompletedTaskCount());

        while (true) {
            Thread.sleep(OptionConverter.toInt(hc.getProp().getProperty("listen.polling.time"), 60) * 1000);
            if (!Compare()) {
                if (rslist.size() != 0) {
                    for (String cn : rslist) {
                        ConfigListener listener = new ConfigListener(cn);
                        listener.run();
                    }
                }
            }
        }
    }

    private static boolean Compare() {
        List<OpenEnvClusterDTO> list = apollo.getClient().getEnvClusterInfo(appid);
        rslist.clear();
        Set<String> clusters = list.get(0).getClusters();
        if (clusters.size() != index.size()) {
            for (String n : clusters) {
                //n.setEnv(env);
                if (!index.contains("数据交换平台智能路由"+n)) {
                    OpenReleaseDTO msg = apollo.getClient().getLatestActiveRelease(appid, env, n, "application");
                    if (msg != null) {
                        rslist.add(n);
                        logger.info(String.format("发现集群%s已发布,但没有对应作业", n));
                    }
                }
            }
            //logger.info(String.format("共发现%s个新增集群，请在%s分钟内完成集群的配置发布！", instances.size(), instances.size() * Integer.parseInt(Runner.hc.getProp().getProperty("create.wait.time"))));
            //Thread.sleep(instances.size() * Integer.parseInt(Runner.hc.getProp().getProperty("create.wait.time")) * 60 * 1000);
            return false;
        } else
            return true;
    }

   /* private static Map<Integer, String> getMysql(String _sql) {
        Map<Integer, String> rslist1 = new HashMap<Integer, String>();
        try {
            conn = DriverManager.getConnection("jdbc:mysql://192.168.191.131:3306/ApolloConfigDB?useSSL=false&serverTimezone=UTC", "root", "acer77e");
            stmt = conn.createStatement();
            String sql = _sql;

            ResultSet rs = stmt.executeQuery(sql);
            int num = 0;
            while (rs.next()) {
                // 通过字段检索
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String date = rs.getString("DataChange_LastTime");

                rslist1.put(num, name);
                num++;

                // 输出数据
                System.out.print("ID: " + id);
                System.out.print(", 集群名称: " + name);
                System.out.print(", 生成时间: " + date);
                System.out.print("\n");
            }
            rs.last();//将指针移动到最后一行数据
            int rowCount = rs.getRow();
            System.out.println("共" + rowCount + "行数据");
            // 完成后关闭
            rs.close();
        } catch (SQLException se) {
            // 处理 JDBC 错误
            se.printStackTrace();
        } catch (Exception e) {
            // 处理 Class.forName 错误
            e.printStackTrace();
        } finally {
            // 关闭资源
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException se2) {
            }// 什么都不做
            try {
                if (conn != null) conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        return rslist1;
    }*/
}
