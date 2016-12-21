'use strict';

var redis = require('redis');
var bluebird = require('bluebird');
var request = require('request');
var mqtt = require('mqtt');
var uuid = require('uuid');
var os = require('os');
var fs = require('fs');
var assert = require('assert');

var debuglog = require('util').debuglog('config');

bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(request, {
  multiArgs: true
});
bluebird.promisifyAll(fs);

var STORE_CONFIG = 3;
var BBCLOUD_BASEURL = process.env.bbcloudBaseUrl;
var MAC_ADDRESS_FILE = '/sys/class/net/wlan0/address';
var TOPIC_DOWNLOAD_START = 'download_manager/download/start';
var TOPIC_DOWNLOAD_ERROR = 'download_manager/download/failed'
var TOPIC_DOWNLOAD_DONE = 'download_manager/download/done';

class AppsConfSDK {

  constructor(opts) {
    var db = opts.db || STORE_CONFIG;

    this.baseUrl = opts.baseUrl || BBCLOUD_BASEURL;

    this.redisClient = opts.redisClient || redis.createClient({
      db
    });
    this.mqttClient = opts.mqttClient || mqtt.connect();

    this.mqttClient.on('connect', this.onMQTTConnect.bind(this));
  }

  onMQTTConnect() {
    this.mqttClient.subscribe(TOPIC_DOWNLOAD_DONE);
    this.mqttClient.subscribe(TOPIC_DOWNLOAD_ERROR);
  }

  getMacAddress() {
    if (this.macAddress) {
      return this.macAddress;
    }
    this.macAddress = fs.readFileSync(MAC_ADDRESS_FILE).toString().trim()
    return this.macAddress;
  }

  ossDownload(bucket, filename) {
    var mqttClient = this.mqttClient;
    return new Promise((resolve, reject) => {
      var correlationId = uuid.v4();
      mqttClient.on('message', this.downloadMessageHandler(correlationId, resolve, reject));
      mqttClient.publish(TOPIC_DOWNLOAD_START, JSON.stringify({
        correlationId,
        bucket,
        filename
      }));
    });
  }

  httpDownload(url) {
    var mqttClient = this.mqttClient;
    return new Promise((resolve, reject) => {
      var correlationId = uuid.v4();
      mqttClient.on('message', this.downloadMessageHandler(correlationId, resolve, reject));
      mqttClient.publish(TOPIC_DOWNLOAD_START, JSON.stringify({
        correlationId,
        url
      }));
    });
  }

  downloadMessageHandler(correlationId, resolve, reject) {
    var self = this;
    return function eventHandler(topic, message) {
      // check topics matched
      if (topic != TOPIC_DOWNLOAD_DONE && topic != TOPIC_DOWNLOAD_ERROR) return;

      try {
        var payload = JSON.parse(message.toString());
      } catch (e) {
        // not related with mismatched payload
        console.warning();
        return;
      }

      // check correlationId matched
      if (payload.correlationId !== correlationId) return;

      // remove listener
      self.mqttClient.removeListener('message', eventHandler);

      if (topic == TOPIC_DOWNLOAD_ERROR) {
        reject();
      }

      if (topic == TOPIC_DOWNLOAD_DONE) {
        resolve(payload.file);
      }
    };
  }

  getConfigFromServerAsync(items) {
    var baseUrl = this.baseUrl;
    var macAddress = this.getMacAddress();
    var requestUrl = `${baseUrl}/api/devices/${macAddress}/configuration`;
    var keys = items.map(item => item.key);
    if (keys.length > 0) {
      return request.getAsync(requestUrl, {
        qs: {
          key: keys
        },
        qsStringifyOptions: {
          arrayFormat: 'repeat'
        },
        json: true
      }).then(function(results) {
        var body = results[1];
        return body;
      });
    }
    return Promise.resolve([]);
  }

  merge(allItems, newItems) {
    var indexedAllItems = this.indexArray(allItems);
    newItems.forEach(function(item) {
      indexedAllItems[item.key] = item;
    });
    return this.unindexObject(indexedAllItems);
  }

  indexArray(arr) {
    return arr.reduce(function(memo, current) {
      memo[current.key] = current;
      return memo;
    }, {});
  }

  unindexObject(obj) {
    return Object.keys(obj).map(function(key) {
      return obj[key];
    });
  }

  flattenPath(items) {
    return items.map((item) => {
      try {
        var value = JSON.parse(item.value);
        if (value.file) return {
          key: item.key,
          value: value.file
        };
        if (value.path) return {
          key: item.key,
          value: value.path
        };
      } catch (e) {}
      if (item.file) return {
        key: item.key,
        value: item.file
      };
      if (item.path) return {
        key: item.key,
        value: item.path
      };
      return item;
    }).reduce(function(memo, current) {
      try {
        var value = JSON.parse(current.value);
        memo[current.key] = value;
      } catch (e) {
        memo[current.key] = current.value;
      }
      return memo;
    }, {});
  }

  downloadAllAsync(tasks) {
    return Promise.all(tasks.map((item) => {
      if (item.url) {
        return this.httpDownload(item.url).then(function(path) {
          return fs.renameAsync(path, item.path);
        });
      } else {
        return this.ossDownload(item.bucket, item.filename).then(function(path) {
          return fs.renameAsync(path, item.path);
        });;
      };
    })).then(function(results) {
      return tasks;
    });
  }

  updateCacheAsync(tasks) {
    if (tasks.length > 0) {
      return this.redisClient.msetAsync(tasks.reduce((memo, current) => {
        memo.push(current.key);
        if (current.path) {
          memo.push(JSON.stringify({
            file: current.path
          }));
        } else {
          memo.push(JSON.stringify(current.value));
        }
        return memo;
      }, []));
    }
    return Promise.resolve();
  }

  findDownloadTask(values) {
    return values.reduce((memo, current) => {
      try {
        var value = JSON.parse(current.value);
        if (value.path && value.url) {
          current.path = value.path;
          current.url = value.url;
          memo.push(current);
        } else if (value.path && value.bucket && value.filename) {
          current.path = value.path;
          current.bucket = value.bucket;
          current.filename = value.filename
          memo.push(current)
        }
      } catch (e) {}
      return memo;
    }, []);
  }

  findUnresolved(values, force) {
    return values.reduce((memo, current, index) => {
      if (force) {
        memo.push(current);

      } else if (current.value === null) {
        memo.push(current);
      }
      return memo;
    }, []);
  }

  findRawValuesAsync(keys) {
    return this.redisClient.mgetAsync(keys).map(function(item, index) {
      return {
        key: keys[index],
        value: item
      };
    });
  }

  retrieve(keys, opts) {
    opts = opts || {};
    var redisClient = this.redisClient;
    var self = this;

    opts.force = opts.force || false;

    return bluebird.coroutine(function*() {

      // 获取配置信息
      var values = yield self.findRawValuesAsync(keys);

      debuglog('=== 获取配置信息 ===\n', values);

      // 收集未缓存的数据
      var unresolved = self.findUnresolved(values, opts.force);

      debuglog('=== 收集未缓存的数据 ===\n', unresolved);

      // 获取未缓存的数据
      var results = yield self.getConfigFromServerAsync(unresolved);

      debuglog('=== 获取未缓存的数据 ===\n', results);

      // 过滤未下载的数据
      var undownloaded = self.findDownloadTask(results);

      debuglog('=== 过滤未下载的数据 ===\n', undownloaded);

      // 针对未缓存的数据进行下载处理
      var task1 = self.downloadAllAsync(undownloaded);

      debuglog('=== 针对未缓存的数据进行下载处理 ===\n', undownloaded);

      // 保存未缓存的 redis 信息
      var task2 = self.updateCacheAsync(results);

      debuglog('=== 保存未缓存的 redis 信息 ===\n', results);

      // 并行等待
      yield Promise.all([task1, task2]);

      // 归并未获取的数据
      var finalAnswer = self.merge(values, results);

      debuglog('=== 归并未获取的数据 ===\n', finalAnswer);

      // 展开文件路径
      return self.flattenPath(finalAnswer);

    })();
  }

  deposit(key, value) {
    console.warn('deposite');
  }

}

function factory(opts) {
  return new AppsConfSDK(opts);
}

module.exports = factory;
