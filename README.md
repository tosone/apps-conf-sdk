# Apps Configuration Manager SDK for Node.js

使用这个 SDK 可以获取缓存在设备中的配置值。也可以自动根据格式触发下载任务。

## 使用方法

```js
var AppsConfSDK = require('AppsConfSDK');
var appsConfSDK = AppsConfSDK();

appsConfSDK.retrieve(['你的 KEY']).then(function(results) {
  console.log(results['你的 KEY']);
});
```

## 配置

```js
AppsConfSDK({
  baseUrl: String,  // 云端访问地址。默认为 http://babacloud.cn
  db: Object        // redis 客户端，不填的话自动新建一个。默认为本机的 db3
});
```
