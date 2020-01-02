package Common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class httpClient {
    private OkHttpClient _client;
    private Logger logger;

    public Properties getProp() {
        return prop;
    }

    private Properties prop;


    public httpClient() throws IOException {
        prop = new Properties();
        prop.load(this.getClass().getResourceAsStream("/META-INF/app.properties"));
        this._client = new OkHttpClient();
        this.logger = LoggerFactory.getLogger(httpClient.class);
    }

    public List<jobId> getMethod(String url) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .get()
                .build();

        Response response = _client.newCall(request).execute();
        String _result = response.body().string();

        logger.info("获取作业信息：" + _result);

        if (_result.contains("[]"))
            return new ArrayList<jobId>();
        else
            return Deserialize(_result);
    }

    private List<jobId> Deserialize(String result) {
        JSONObject jsonobj = JSON.parseObject(result);
        String array = jsonobj.get("jobs").toString();
        List<jobId> jobs = JSON.parseArray(array, jobId.class);
        System.out.println("jobs:" + jobs);
        return jobs;
    }
}
