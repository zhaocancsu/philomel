# philomel
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;philomel是一个类似于spark application提交管理的server,它提供restful接口，你可以通过http post请求来提交你的spark任务，目前使用scala语言编写，也支持java语言的开发,本身是基于spring boot来构建，它可以作为spark工程化实践的一种参考，不同于submit脚本的提交方式，当工程编译构建完成并迅速启动后，它本身可以作为一个driver存在,当然在你启动该服务的机器上要正确设置host以使spark的driver进程可以和master与worker进行通信。<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;目前，代码实现部分仅支持standalone模式的spark集群配置方式，每个spark任务使用的资源(总核数和每个executor内存大小)可通过http参数设置。

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
 
