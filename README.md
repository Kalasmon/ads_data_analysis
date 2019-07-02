# GOOGLE x FACEBOOK x APPSFLYER 数据自动抓取和串联示例

流量团队在初次搭建自己的广告数据BI时，总会先遇到这几个问题：

1）如何自动抓获数据？

2）如何不重不漏地整合不同平台的数据？


主流的广告平台（Google Facebook Twitter Snapchat等）和主流的追踪平台（appsflyer ajust等）都有非常成熟的API，本项目以常用的两个广告平台和追踪平台做示例，展示数据的抓取和整合过程。

## 一、Google 部分注意事项：

1）API如果是Basic权限，每天请求次数额度为1w次；

2）Google UAC 暂不支持直接导出 install 字段，因此最佳处理方式是另外以“conversion name”多导一次，通过筛选需要的conversion（如first open等）并匹配至Campaign Performance Report；

3）Google UAC 目前导数的最小层级是campaign；

## 二、Facebook 部分注意事项：

暂无

## 三、Appsflyer 部分注意事项：

1）案例里用的是 masterAPI V5，此版本导数的最小维度是 adsetID（v4 之前为 adid），另外取数的窗口会比面板上慢一天左右；


在完成上述平台的数据抓取和储存后，在相应的分析平台对应调取、匹配和计算即可。
