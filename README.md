philomel
========
philomel是一个类似于spark application提交管理的server,它提供restful接口，
你可以通过http post请求来提交你的spark任务，目前使用scala语言编写，也支持java语言的开发,本身是基于spring boot来构建，它可以作为spark工程化实践的一种参考，不同于submit脚本的提交方式，当工程编译构建完成并迅速启动后，它本身可以作为一个driver存在，当然在你启动该服务的机器上要正确设置host以使spark的driver进程可以和master与worker进行通信。<br>
    目前，代码实现部分仅支持standalone模式的spark集群配置方式，每个spark任务使用的资源(总核数和每个executor内存大小)可通过http参数设置

--------
