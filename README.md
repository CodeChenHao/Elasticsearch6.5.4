# Elasticsearch 6.5.4 Java 操作示例

## 项目简介

本项目是一个用于演示如何通过 Java 操作 Elasticsearch 6.5.4 版本的示例代码库。项目包含了 Elasticsearch 的核心操作，包括索引管理和文档操作，为开发者提供了一套完整的 Elasticsearch Java 客户端使用示例。

## 项目结构

```
Elasticsearch6.5.4/
├── src/
│   └── main/
│       └── java/
│           └── com/
│               └── chenhao/
│                   ├── entity/       # 实体类
│                   │   └── Person.java
│                   ├── test/         # 测试类
│                   │   ├── Demo1.java  # 客户端连接测试
│                   │   ├── Demo2.java  # 索引操作测试
│                   │   └── Demo3.java  # 文档操作测试
│                   └── utils/        # 工具类
│                       └── ESClient.java  # Elasticsearch 客户端工具
├── .gitignore
├── README.md
└── pom.xml
```

## 技术栈

- Java 8
- Elasticsearch 6.5.4
- Elasticsearch Rest High Level Client 6.5.4
- JUnit 4.12
- Lombok 1.16.22
- Jackson Databind 2.10.2

## 功能说明

### 1. 索引操作
- 创建带 Mappings 的索引
- 判断索引是否存在
- 删除索引

### 2. 文档操作
- 创建文档（自动生成 ID）
- 创建文档（自定义 ID）
- 修改文档（非覆盖式更新）
- 删除文档
- 批量创建文档（自动生成 ID）
- 批量创建文档（自定义 ID）
- 批量删除文档

## 环境要求

- JDK 8 或更高版本
- Maven 3.0 或更高版本
- Elasticsearch 6.5.4 服务端

## 如何使用

### 1. 配置 Elasticsearch 连接

修改 `src/main/java/com/chenhao/utils/ESClient.java` 文件中的 Elasticsearch 服务地址：

```java
// 封装ES的地址
HttpHost httpHost = new HttpHost("192.168.3.123", 9200);
```

### 2. 构建项目

在项目根目录执行以下命令：

```bash
mvn clean package
```

### 3. 运行测试

使用 IDE 运行测试类中的测试方法，或使用 Maven 命令运行测试：

```bash
mvn test
```

## 代码示例

### 1. 初始化 Elasticsearch 客户端

```java
// 获取 Elasticsearch 客户端
RestHighLevelClient client = ESClient.getClient();
```

### 2. 创建索引（带 Mappings）

```java
// 构造 settings
Settings.Builder settings = Settings.builder()
                                    .put("number_of_shards", 3)
                                    .put("number_of_replicas", 1);

// 构造 mappings
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

// 构建 Request 对象
CreateIndexRequest request = new CreateIndexRequest("person")
                                 .settings(settings)
                                 .mapping("man", mappings);

// 发送 Request 创建索引
CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
```

### 3. 创建文档

```java
// 创建实体对象
Person person = new Person(1, "张三", 18, new Date());
String json = mapper.writeValueAsString(person);

// 构建 Request 对象（自动生成 ID）
IndexRequest request = new IndexRequest("person", "man");
request.source(json, XContentType.JSON);

// 发送 Request 创建文档
IndexResponse response = client.index(request, RequestOptions.DEFAULT);
```

### 4. 批量操作

```java
// 创建多个实体对象
Person p1 = new Person(1, "张三", 18, new Date());
Person p2 = new Person(2, "李四", 28, new Date());
Person p3 = new Person(3, "王五", 38, new Date());

// 转换为 JSON 字符串
String json1 = mapper.writeValueAsString(p1);
String json2 = mapper.writeValueAsString(p2);
String json3 = mapper.writeValueAsString(p3);

// 构建批量请求
BulkRequest request = new BulkRequest();
request.add(new IndexRequest("person", "man").source(json1, XContentType.JSON));
request.add(new IndexRequest("person", "man").source(json2, XContentType.JSON));
request.add(new IndexRequest("person", "man").source(json3, XContentType.JSON));

// 发送批量请求
BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
```

## 注意事项

1. 确保 Elasticsearch 6.5.4 服务已经启动并且可以访问
2. 修改 `ESClient.java` 中的连接地址为你的 Elasticsearch 服务地址
3. 本项目使用的是 Elasticsearch 6.5.4 版本，与其他版本可能存在 API 差异
4. 测试方法需要按照顺序执行，例如先创建索引，再操作文档
5. 项目使用 JUnit 4 进行测试，每个测试方法都可以独立运行

## 许可证

本项目为学习示例，无特定许可证限制，可自由使用和修改。