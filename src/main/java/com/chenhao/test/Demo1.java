package com.chenhao.test;

import com.chenhao.utils.ESClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

public class Demo1 {
    /*测试获取客户端对象是否成功*/
    @Test
    public void testConnect(){
        RestHighLevelClient client = ESClient.getClient();
        System.out.println("OK");
    }
}
