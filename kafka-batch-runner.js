const async = require('async');
const kafka = require('kafka-node');

const ConsumerGroup = kafka.ConsumerGroup;
function Node(data) {
  this.data = data;
}

/**
* Linked list.
*/
function LinkedList() {
  this.length = 0;
    // this.head = undefined;
    // this.tail = undefined;
}
LinkedList.prototype.pop = function pop() {
  if (!this.head) {
    return undefined;
  }
  this.length--;
  const retVal = this.head.data;
  this.head = this.head.next;
  return retVal;
};
LinkedList.prototype.add = function add(data) {
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
  function debug(...rest) {
    if (config.debug) {
      console.log(rest);
    }
  }
  const messageBuffer = new LinkedList();
  const consumerGroup = new ConsumerGroup(options, topics);

  function run(callback) {
    // Emit each message in the message buffer.
    async.timesSeries(messageBuffer.length, (n, next) => {
      node.send({
        payload: messageBuffer.pop(),
        cb: next,
      });
    }, doneProcessing);

    function doneProcessing(err) {
      if (err) {
        debug('An error has occured while processing a message. Quitting. Error = ', err);
        process.exit(1);
      }
      callback();
    }
  }

  consumerGroup.on('message', (msg) => {
    debug('Received message. Adding it to the buffer -', msg);
    messageBuffer.add(msg);
  });

  consumerGroup.on('done', () => {
    debug('Done one fetch request. Pausing the stream.');
    consumerGroup.pause();
    run(() => {
      debug('Done processing one batch. Commiting now.');
      consumerGroup.commit(true, (err) => {
        if (err) {
          debug('Error occured while commiting a batch. Quitting. Error =', err);
          process.exit(1);
        }
        debug('Done processing records. Resuming the stream');
        consumerGroup.resume();
      });
    });
  });
}
module.exports = KafkaBatchRunner;
