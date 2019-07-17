# Ecoweb
### 一个Node.js的高扩展性的服务框架，集成了集群部署、微服务架构所需的功能，用最小的代码实现常见的web业务。
Ecoweb基于MongoDB，Redis，Express 4.x以及APIServer（基于原生http库开发的极简化API服务库），经过数个商业项目凝练而来。  

### 特性
1. 合理的项目文件结构，区分路由逻辑和API逻辑
2. 路由和API可定义访问权限、限流
3. API定义支持常用的数据校验（如字符，数字，手机号等），支持必须参数和可选参数设定
4. 提供API调试工具，自动显示API描述和参数说明
5. 支持多环境配置, 可根据启动参数切换运行环境, 如dev, test, production等, 不同的环境使用不同的配置文件，由开发者自由定义
6. 使用Mongoose操作数据库，简化了Schema定义流程，简化了Model使用方式
7. 封装了socket.io可以实现基本的websocket实时数据交互
8. 集成一些常见的web服务功能，如用户权限维护，邮件发送，短信发送/验证码检查等
9. 面向微服务架构，支持集群化部署，内置了基于Redis的RPC调用系统

### 文档
主页：[Ecoweb](https://snoopyjoy.github.io/ecoweb/)

### 鸣谢
本项目fork自Jay Liang的[weroll](https://github.com/jayliang701/weroll), 在weroll框架上进行了部分改进。