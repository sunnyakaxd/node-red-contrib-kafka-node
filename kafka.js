/**
 * Created by fwang1 on 3/25/15.
 */
module.exports = function (RED) {
    /*
     *   Kafka Producer
     *   Parameters:
     - topics
     - zkquorum(example: zkquorum = “[host]:2181")
     */
  function kafkaNode(config) {
    RED.nodes.createNode(this, config);
    const topic = config.topic;
    const clusterZookeeper = config.zkquorum;
    const debug = (config.debug == 'debug');
    const node = this;
    const kafka = require('kafka-node');
    const HighLevelProducer = kafka.HighLevelProducer;
    const Client = kafka.Client;
    const topics = config.topics;
    const client = new Client(clusterZookeeper);

    try {
      this.on('input', (msg) => {
        let payloads = [];

                // check if multiple topics
        if (topics.indexOf(',') > -1) {
          const topicArry = topics.split(',');

          for (i = 0; i < topicArry.length; i++) {
            payloads.push({ topic: topicArry[i], messages: msg.payload });
          }
        } else {
          payloads = [{ topic: topics, messages: msg.payload }];
        }

        producer.send(payloads, (err, data) => {
          if (err) {
            node.error(err);
          }
          node.log(`Message Sent: ${data}`);
        });
      });
    } catch (e) {
      node.error(e);
    }
    var producer = new HighLevelProducer(client);
    this.status({
      fill: 'green',
      shape: 'dot',
      text: `connected to ${clusterZookeeper}`,
    });
  }

  RED.nodes.registerType('kafka', kafkaNode);

    /*
     *   Kafka Consumer
     *   Parameters:
     - topics
     - groupId
     - zkquorum(example: zkquorum = “[host]:2181")
     */
  function kafkaInNode(config) {
    RED.nodes.createNode(this, config);

    const node = this;
    const highLevelConsumer = false;
    const kafka = require('kafka-node');
    const HighLevelConsumer = kafka.HighLevelConsumer;
    const Client = kafka.Client;
    let topics = String(config.topics);
    const clusterZookeeper = config.zkquorum;
    const groupId = config.groupId;
    const debug = true;// (config.debug == 'debug');
    const client = new Client(clusterZookeeper);
    const cgTopics = topics.split(',');
    topics = cgTopics.map(topic => ({
      topic,
    }));
//    return console.log(JSON.stringify(config));
//        var topicJSONArry = [];
//        // check if multiple topics
//        if (topics.indexOf(",") > -1){
//            var topicArry = topics.split(',');
//            if (debug) {
//                console.log(topicArry)
//                console.log(topicArry.length);
//            }
//
//            for (i = 0; i < topicArry.length; i++) {
//                if (debug) {
//                    console.log(topicArry[i]);
//                }
//                topicJSONArry.push({topic: topicArry[i]});
//            }
//            topics = topicJSONArry;
//        }
//        else {
//            topics = [{topic:topics}];
//        }

    if (highLevelConsumer) {
      var options = {
        groupId,
        autoCommit: config.autoCommit,
        autoCommitMsgCount: 10,
      };
      try {
        const consumer = new HighLevelConsumer(client, topics, options);
        this.log('Consumer created...');
        this.status({
          fill: 'green',
          shape: 'dot',
          text: `connected to ${clusterZookeeper}`,
        });

        consumer.on('message', (message) => {
          if (debug) {
            console.log(message);
            node.log(message);
          }
          const msg = { payload: message };
          node.send(msg);
        });

        consumer.on('error', (err) => {
          console.error(err);
        });
      } catch (e) {
        node.error(e);
      }
    } else {
      try {
        var options = {
          host: clusterZookeeper,
          zk: {
            sessionTimeout: 10000,
          },   // put client zk settings if you need them (see Client)
          batch: undefined, // put client batch settings if you need them (see Client)
            //  ssl: true, // optional (defaults to false) or tls options hash
          groupId,
          autoCommit: false,
          sessionTimeout: 15000,
              // An array of partition assignment protocols ordered by preference.
              // 'roundrobin' or 'range' string for built ins (see below to pass in custom assignment protocol)
          protocol: ['roundrobin'],

              // Offsets to use for new groups other options could be 'earliest' or 'none' (none will emit an error if no offsets were saved)
              // equivalent to Java client's auto.offset.reset
          fromOffset: 'earliest', // default

              // how to recover from OutOfRangeOffset error (where save offset is past server retention) accepts same value as fromOffset
          outOfRangeOffset: 'earliest', // default
          migrateHLC: false,    // for details please see Migration section below
          fetchMaxWaitMs: 100,
          fetchMinBytes: 1,
          fetchMaxBytes: 10240,
        };
        const kafkaBatchRunner = require('./kafka-batch-runner');
        kafkaBatchRunner(node, options, cgTopics, {
          debug,
        });
      } catch (e) {
        node.error(e);
      }
    }
  }

  RED.nodes.registerType('kafka in', kafkaInNode);
};
