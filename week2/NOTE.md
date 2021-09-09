```java
Hadoop RPC作业
根据文档中的示例，完成一个类似的 RPC 函数，要求：
输入你的真实学号，返回你的真实姓名
输入学号 20210000000000，返回 null
输入学号 20210123456789，返回心心

public String findName(String studentId) {
    if ("20210123456789".equals(studentId)) return "心心";
    if ("20210000000000".equals(studentId)) return "null";
    if ("G20200343150041".equals(studentId)) return "cal";
    return "其他";
}
```

