# 基于GeoTools的Phoenix空间数据管理引擎

项目基于 [`GeoTools`](https://github.com/geotools/geotools) 的GIS应用框架，完成对其 `jdbc` 模块关于在Phoenix的SQL中间件上的空间数据管理的扩展，目前支持空间数据类型中 `POINT` 数据的管理。

![GeoTools](https://github.com/geotools/geotools/raw/master/README-geotools-logo.png)

[`GeoTools`](http://geotools.org/)是Java语言编写的开源GIS工具包。该项目已有十多年历史，生命力旺盛，代码非常丰富，包含多个开源GIS项目，并且基于标准的GIS接口。GeoTools主要提供各种GIS算法，各种数据格式的读写和显示。在数据显示方面要差一些，只是用Swing实现了地图的简单查看和操作。但是用户可以根据GeoTools提供的算法自己实现地图的可视化。GeoTools用到的两个较重要的开源GIS工具是JTS和GeoAPI。前者主要实现各种GIS拓扑算法，也是基于GeoAPI的。但是由于两个工具包的GeoAPI分别采用不同的Java代码实现，所以在使用时需要相互转化。GeoTools又根据两者定义了部分自己的API，所以代码显得臃肿，有时容易混淆。由于GeoAPI进展缓慢，GeoTools自己对其进行了扩充。另外，GeoTools现在还只是基于2D图形的，缺乏对3D空间数据算法和显示的支持。

![Phoenix](http://phoenix.apache.org/images/phoenix-logo-small.png)

[`Apache Phoenix`](http://phoenix.apache.org/)是建立在分布式开源NoSQL数据库 `Apache Hbase` 之上的关系数据库层中间件，其能将对类似于关系型数据的获取语法编译成为对于Hbase上的数据的Scan语法，并且其查询使用许多Hbase的相关特性以提高查询性能，并尽可能多的将工作放到集群上并行执行。

## 安装

项目依托 `maven` 进行依赖管理，然而作者未能在全球maven仓库中找到 `Phoenix` 关于 `Hbase` 的驱动文件找到镜像，故采用本地指定加载方式。驱动文件放置于：

    lib/phoenix-4.4.0-hbase-0.98-client.jar
    
进入项目 `jdbc-phoenix` 目录下，使用如下命令安装：

    mvn clean install
    
## 内容摘要

此项目完成对于空间数据类型中点数据的管理。由于Hbase不支持空间类型数据的存储及管理，所以需要对其进行空间扩展。针对 `POINT` 空间类型数据，利用 `WKT` 形式对其进行存储，并在扩展中自动加入 `GEOHASH` 列并在其上建立索引，目的在于将对二维点数据的查询转换为高效的对于Hbase上的一维数据列查询。例如当应用层关于点数据列名为 `GEOPOINT` 时，则由扩展新增的一维索引列名为 `GEOPOINT_GEOHASH`。

**点数据转化为GEOHASH算法**

```java
/**
 * 函数功能：经纬度坐标转long类型的GeoHash
 *
 * @param longitude 经度
 * @param latitude  纬度
 * @return 生成的long类型的GeoHash值
*/
public static long longAndLatiToGeohash(double longitude, double latitude) {
    boolean isEven = true;
    double minLat = -90.0, maxLat = 90.0;
    double minLon = -180.0, maxLon = 180.0;
    long bit = 0x8000000000000000L;
    long g = 0x0L;

    long target = 0x8000000000000000L >>> (5 * MAX_HASH_LENGTH);
    while (bit != target) {
        if (isEven) {
            double mid = (minLon + maxLon) / 2;
            if (longitude >= mid) {
                g |= bit;
                minLon = mid;
            } else
                maxLon = mid;
        } else {
            double mid = (minLat + maxLat) / 2;
            if (latitude >= mid) {
                g |= bit;
                minLat = mid;
            } else
                maxLat = mid;
        }

        isEven = !isEven;
        bit >>>= 1;
    }
    return g;
}
```

当对点数据进行查询时，系统会先用 `GeoHash` 对所查询范围进行进行粗过滤，然后利用用户 `UDF`（用户自定义函数）进行精过滤，最后筛选出符合条件的点数据。

## 联系方式

欢迎反馈问题，作者邮箱地址：`daqinzhidi@163.com`