package com.chenhao.test;

import com.chenhao.entity.Person;
import com.chenhao.utils.ESClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

/*文档相关操作*/
public class Demo3 {
    ObjectMapper mapper = new ObjectMapper();
    RestHighLevelClient client = ESClient.getClient();
    String index = "person";
    String type = "man";

    /*创建文档-自动生成Id*/
    @Test
    public void createDocumentByAutoId() throws IOException {
        Person person = new Person(1,"张三",18,new Date());
        String json = mapper.writeValueAsString(person);

        // 构建Request对象
        IndexRequest request = new IndexRequest(index,type);
        request.source(json, XContentType.JSON);

        // 发送Request创建索引
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        // 输出响应结果
        System.out.println(response);
    }

    /*创建文档-自定义Id*/
    @Test
    public void createDocumentByCustomId() throws IOException {
        Person person = new Person(1,"张三",18,new Date());
        String json = mapper.writeValueAsString(person);

        // 构建Request对象
        IndexRequest request = new IndexRequest(index,type,person.getId().toString());
        request.source(json, XContentType.JSON);

        // 发送Request创建索引
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        // 输出响应结果
        System.out.println(response);
    }

    /*修改文档-非覆盖式更新*/
    @Test
    public void updateDocumentByNotFull() throws IOException {
        HashMap<String, Object> updateMap = new HashMap<>();
        updateMap.put("name","李四");
        String documentId = "1";

        // 构建Request对象
        UpdateRequest request = new UpdateRequest(index,type,documentId);
        request.doc(updateMap);

        // 发送Request创建索引
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);

        // 输出响应结果
        System.out.println(response);
    }

    /*删除文档*/
    @Test
    public void deleteDocument() throws IOException {
        String documentId = "1";

        // 构建Request对象
        DeleteRequest request = new DeleteRequest(index,type,documentId);

        // 发送Request创建索引
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);

        // 输出响应结果
        System.out.println(response);
    }


    /*批量创建文档-自动生成Id*/
    @Test
    public void bulkCreateDocumentByAutoId() throws IOException {
        Person p1 = new Person(1,"张三",18,new Date());
        Person p2 = new Person(2,"李四",28,new Date());
        Person p3 = new Person(3,"王五",38,new Date());

        String json1 = mapper.writeValueAsString(p1);
        String json2 = mapper.writeValueAsString(p2);
        String json3 = mapper.writeValueAsString(p3);

        // 构建Request对象
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest(index,type).source(json1,XContentType.JSON));
        request.add(new IndexRequest(index,type).source(json2,XContentType.JSON));
        request.add(new IndexRequest(index,type).source(json3,XContentType.JSON));

        // 发送Request创建索引
        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);

        // 输出响应结果
        System.out.println(response);
    }

    /*批量创建文档-自定义Id*/
    @Test
    public void bulkCreateDocumentByCustomId() throws IOException {
        Person p1 = new Person(1,"张三",18,new Date());
        Person p2 = new Person(2,"李四",28,new Date());
        Person p3 = new Person(3,"王五",38,new Date());

        String json1 = mapper.writeValueAsString(p1);
        String json2 = mapper.writeValueAsString(p2);
        String json3 = mapper.writeValueAsString(p3);

        // 构建Request对象
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest(index,type,p1.getId().toString()).source(json1,XContentType.JSON));
        request.add(new IndexRequest(index,type,p2.getId().toString()).source(json2,XContentType.JSON));
        request.add(new IndexRequest(index,type,p3.getId().toString()).source(json3,XContentType.JSON));

        // 发送Request创建索引
        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);

        // 输出响应结果
        System.out.println(response);
    }


    /*批量删除文档*/
    @Test
    public void bulkDeleteDocument() throws IOException {
        String documentId = "1";

        // 构建Request对象
        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest(index,type,"1"));
        request.add(new DeleteRequest(index,type,"2"));
        request.add(new DeleteRequest(index,type,"3"));


        // 发送Request创建索引
        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);

        // 输出响应结果
        System.out.println(response);
    }
}
