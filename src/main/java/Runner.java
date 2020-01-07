import Common.ApolloOpen;
import Common.httpClient;
import com.ctrip.framework.apollo.openapi.dto.OpenEnvClusterDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.sql.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Runner {
    static private Connection conn = null;
    static private Statement stmt = null;
    static public Map<String, String> index = new HashMap<String, String>();
    static private List<String> rslist = new ArrayList<String>();
    static public httpClient hc = null;
    static private ApolloOpen apollo;
    static private String appid;
    static public final Object o=new Object();

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(Runner.class);
        hc = new httpClient();
        CheckJob cj=new CheckJob();

        Class.forName("com.mysql.cj.jdbc.Driver");

        appid = hc.getProp().getProperty("apollo.appid");
        apollo = new ApolloOpen(hc.getProp().getProperty("apollo.portalUrl"), hc.getProp().getProperty("apollo.token"));

        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 5, 200,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1));

        executor.execute(cj);
        logger.info("线程池中线程数目：" + executor.getPoolSize() + "，队列中等待执行的任务数目：" +
                executor.getQueue().size() + "，已执行玩别的任务数目：" + executor.getCompletedTaskCount());

        while (true) {
            Thread.sleep(1 * 1000);
            if (!Compare()) {
                for (int i = 0; i != rslist.size(); i++) {
                    String cn = rslist.get(i);
                    ConfigListener listener = new ConfigListener(cn);
                    listener.run();
                }
            }
        }
    }

    private static boolean Compare() {
        List<OpenEnvClusterDTO> list = apollo.getClient().getEnvClusterInfo(appid);
        rslist.clear();
        if (list.get(0).getClusters().size() != index.size()) {
            List<String> instances = new ArrayList<String>();
            for (OpenEnvClusterDTO n : list) {
                //n.setEnv("dev");
                for (String n1 : n.getClusters()
                        ) {
                    synchronized (o) {
                        if (!index.containsKey(n1)) {
                            instances.add(n1);
                        }
                    }
                }
            }
            rslist = instances;
            return false;
        } else
            return true;
    }

    private static Map<Integer, String> getMysql(String _sql) {
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
    }


}
