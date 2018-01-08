package com.github.tiger.kafka.consumer.handler;

import com.github.tiger.kafka.common.Constants;
import com.github.tiger.kafka.common.URL;
import com.github.tiger.kafka.utils.HttpClientUtil;
import com.google.common.collect.Maps;
import org.apache.http.HttpHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Map;

import static com.github.tiger.kafka.common.Protocol.HttpMethod;

/**
 * @author liuhongming
 */
public class HttpDispatcher extends AbstractConsumerHandler<Collection<ConsumerRecord<String, String>>> {

    private static final Logger logger = LoggerFactory.getLogger(HttpDispatcher.class);

    private static Map<String, String> headers = Maps.newHashMap();

    static {
        headers.put(HttpHeaders.CONTENT_ENCODING, "UTF-8");
    }

    private URL httpUrl;
    private HttpMethod method;


    public HttpDispatcher(URL httpUrl) {
        this(httpUrl, HttpMethod.POST);
    }

    public HttpDispatcher(URL httpUrl, HttpMethod method) {
        this.httpUrl = httpUrl;
        this.method = method;
    }

    @Override
    public void handle(Collection<ConsumerRecord<String, String>> records) throws Exception {
        for (ConsumerRecord<String, String> record : records) {
            Map<String, String> parameters = httpUrl.getParameters();
            parameters.put(Constants.DEFAULT_HTTP_DATA_KEY, record.value());
            doDispatch();
        }
    }

    private void doDispatch() throws URISyntaxException, UnsupportedEncodingException {
        logger.info("Request: url={}, parameters={}", httpUrl.getPath(), httpUrl.getParameters());
        String responseBody = "";
        if (method == HttpMethod.GET) {
            responseBody = doGet();
        } else if (method == HttpMethod.POST) {
            responseBody = doPost();
        }
         logger.info("Response: body={}", responseBody);
    }

    private String doGet() throws URISyntaxException {
        headers.put(HttpHeaders.CONTENT_TYPE, "text/plain");
        return HttpClientUtil.get(httpUrl.getPath(), headers, httpUrl.getParameters());
    }

    private String doPost() throws UnsupportedEncodingException {
        headers.put(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
        return HttpClientUtil.post(httpUrl.getPath(), headers, httpUrl.getParameters());
    }

}
