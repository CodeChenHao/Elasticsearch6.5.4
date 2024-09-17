package com.chenhao.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class ESClient {

    public static RestHighLevelClient getClient(){
        // 封装ES的地址
        HttpHost httpHost = new HttpHost("192.168.3.123", 9200);
        // 根据ES地址,获取ES客户端构建对象
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        // 通过ES构建对象构建ES客户端对象
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        return restHighLevelClient;
    }
}
