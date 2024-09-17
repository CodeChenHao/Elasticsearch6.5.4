package com.chenhao.test;

import com.chenhao.utils.ESClient;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.io.IOException;

/* 索引的相关操作 */
public class Demo2 {
    RestHighLevelClient client = ESClient.getClient();
    String index = "person";
    String type = "man";

    /*测试创建带有Mapping的索引*/
    @Test
    public void testCreateIndex() throws IOException {
        // 构造settings
        Settings.Builder settings = Settings.builder()
                                            .put("number_of_shards", 3)
                                            .put("number_of_replicas", 1);
        // 构造mappings
        XContentBuilder mappings = JsonXContent.contentBuilder()
                                                      .startObject()
                                                          .startObject("properties")
                                                              .startObject("name")
                                                                    .field("type", "text")
                                                              .endObject()
                                                              .startObject("age")
                                                                    .field("type", "integer")
                                                              .endObject()
                                                              .startObject("birthday")
                                                                    .field("type", "date")
                                                                    .field("format", "yyyy-MM-dd")
                                                              .endObject()
                                                          .endObject()
                                                      .endObject();

        // 构建Request对象
        CreateIndexRequest request = new CreateIndexRequest(index)
                                         .settings(settings)
                                         .mapping(type,mappings);

        // 发送Request创建索引
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);

        // 输出响应结果
        System.out.println(response);
    }

    /*检查索引是否存在*/
    @Test
    public void testCheckIndexExists() throws IOException {
        // 构建Request对象
        GetIndexRequest request = new GetIndexRequest();
        request.indices(index);

        // 发送Request创建索引
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);

        // 输出响应结果
        System.out.println(exists);
    }

    /*删除索引*/
    @Test
    public void testDeleteIndex() throws IOException {
        // 构建Request对象
        DeleteIndexRequest request = new DeleteIndexRequest();
        request.indices(index);

        // 发送Request创建索引
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);

        // 输出响应结果
        System.out.println(response);
    }
}
