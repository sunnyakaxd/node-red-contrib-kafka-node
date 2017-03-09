var async = require('async');
var kafka = require('kafka-node');
var ConsumerGroup = kafka.ConsumerGroup;
function Node(data) {
  this.data = data;
}
function LinkedList() {
  this.length = 0;
    //this.head = undefined;
    //this.tail = undefined;
};
LinkedList.prototype.pop = function(data) {
  if (!this.head) {
    return;
  }
  this.length--;
  var retVal = this.head.data;
  this.head = this.head.next;
  return retVal;
};
LinkedList.prototype.add = function(data) {
  if (data === undefined) {
    throw new Error('Cannot insert undefined into linked list');
  }
  if (!this.head) {
    this.head = this.tail = new Node(data);
  } else {
    this.tail = this.tail.next = new Node(data);
  }
  this.length++;
};
function KafkaBatchRunner(node, options, topics, config) {
  var consumerGroup = this.consumerGroup = new ConsumerGroup(options, topics);
  var versionDict = {};
  versionDict.__cur = 0;
  var running = false;
  function next(version) {
    if (versionDict.__cur == version) {
      versionDict.__cur = (versionDict.__cur + 1) % 10;
    }
  }
  function run() {
    if (running) {
      return;
    }
    running = true;
    var thisRun = versionDict.__cur;
    next(thisRun);
    var list = versionDict[thisRun].list;
//        var cbArray = new Array(list.length)
    async.times(list.length, function(n, next) {
      node.send({
        payload: list.pop(),
        cb: next,
      });
    }, function(err) {
      if (err) {
        process.exit(1);
      }
      delete versionDict[thisRun];
      config.debug && console.log('Resuming ', consumerGroup + '');
      consumerGroup.commit(true, function(err, data) {
        if (err) {
          config.debug && console.log('Commit failed, purging core-svcs');
          process.exit(1);
        }
        config.debug && console.log('Batch commit successfull');
      });
      consumerGroup.resume();
    });
  }
  consumerGroup.on('message', function(msg) {
    config.debug && console.log('got msg');
    var myVersion = versionDict.__cur;
    var curvDict = versionDict[myVersion] = (versionDict[myVersion] || {
      list: new LinkedList(),
    });
    curvDict.list.add(msg);
    //console.log('calling pause');
    consumerGroup.pause();
    //console.log('calling run');
    process.nextTick(run);
  });
}
module.exports = KafkaBatchRunner;

