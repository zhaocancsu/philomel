# philomel
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;philomel是一个类似于spark application提交管理的server,它提供restful接口，你可以通过http post请求来提交你的spark任务，目前使用scala语言编写，也支持java语言的开发,本身是基于spring boot来构建，它可以作为spark工程化实践的一种参考，不同于submit脚本的提交方式，当工程编译构建完成并迅速启动后，它本身可以作为一个driver存在,当然在你启动该服务的机器上要正确设置host以使spark的driver进程可以和master与worker进行通信。<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;目前，代码实现部分仅支持standalone模式的spark集群配置方式，每个spark任务使用的资源(总核数和每个executor内存大小)可通过http参数设置。<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;当前philomel仅仅作为单独的一个进程存在，并且它是无状态的(在功能调用上)，如果把它看作一种计算资源存在，并且在未来业务使用上可以进行弹性伸缩，那么应将它编译后通过DockerFile制作成镜像，然后使用K8S来管理使用。

<table>
       <tr>
           <td>author</td>
           <td>email</td>
        </tr>
        <tr>
            <td>soy</td>
            <td>zhaocan@migu.cn</td>
        </tr>
 </table>
 
 
 
 ### 环境配置
   * hadoop2.7.3
   * spark2.3.0
   * hive2.3.2 
   * hbase1.4.2
   * phoenix-4.14-hbase-1.4

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;本工程实例代码中侧重的是spark sql的使用，本质是通过操作hive表项来实现或进行后续的业务处理，hbase表统一通过phoenix的进行操作管理，但从sql的使用上来说，我们在philomel中想更少地或并不打算直接通过phoenix的api来对hbase中的数据进行操作，也想统一地通过操作hive表来完成，所以需要在hive中创建一个外部表来对已存在的hbase表进行映射，在实践过程中最好需要遵循以下2条的原则：
 1. 创建hbase:在phoenix shell
 2. 在hive中创建external table来映射hbase表
   
可以参考[此记录](https://www.jianshu.com/p/09c30d2074d6)简单地了解在hive中创建映射phoenix hbase表项的操作步骤

 ### 功能接口描述
![接口](https://github.com/zhaocancsu/philomel/blob/master/rest.png)


 ### 消息类型
   * spark context创建消息(与功能接口同步)
   * spark任务执行结果消息:异步通知任务执行结果
   * 进程启动成功消息:通知位置信息(ip和port)给外部来作driver资源请求的负载均衡
 
 ### 原理说明
   * philomel启动的进程中，仍然是按照One Spark Context，One JVM的方式工作的
   * 启动多个philomel实例，并且汇报实例信息给其他组件(driver资源管理),信息包括ip、port以及最后更新时间等，调度使用实例时可以参考某个主机上实例个数、实例最后使用时间等维度来进行分发请求
   * 可以把启动的实例归为进程池的概念
   * 每个实例固定对应一个spark+hadoop环境，当然它本身在配置上支持不同spark+hadoop的环境，这需要修改项目中的hdfs-site.xml,core-site.xml等配置文件
   * 可以通过spark sql操作hive表或hbase表，操作hbase表需要在hive中创建外部表来映射
   
 ### jar说明
   * phoenix-4.14.0-HBase-1.4-hive.jar和phoenix-4.14.0-HBase-1.4-client.jar是作为第三方包拷贝到classpath,不在pom中
   * 为解决版本冲突,phoenix-4.14.0-HBase-1.4-hive.jar手动删除了org/slf4j、io目录
