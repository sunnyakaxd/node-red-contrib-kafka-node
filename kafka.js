/**
 * Created by fwang1 on 3/25/15.
 */

 const kafka = require('kafka-node');
 const kafkaBatchRunner = require('./kafka-batch-runner');
 const ConsistentHash = require('consistent-hash');

 module.exports = function kafkaNodes(RED) {
    /*
     *   Kafka Producer
     *   Parameters:
     - topics
     - zkquorum(example: zkquorum = “[host]:2181")
     */
   function kafkaNode(config) {
     RED.nodes.createNode(this, config);
     const node = this;
     const disabled = config.disabled;
     if (disabled) {
       node.status({
         fill: 'red',
         shape: 'dot',
         text: 'Disabled',
       });
       node.on('input', (msg) => {
         node.send({
           payload: {
             sent: false,
             error: 'Node disabled',
             msg,
           },
         });
       });
       return;
     }
     const retryTimeouts = new Set();
     const oldRetryTimeoutsAdd = retryTimeouts.add;
     retryTimeouts.add = (fn, timeout) => {
       const thisTimeOut = setTimeout(() => {
         retryTimeouts.delete(thisTimeOut);
         fn();
       }, timeout);
       oldRetryTimeoutsAdd.call(retryTimeouts, thisTimeOut);
     };
     const clusterZookeeper = config.zkquorum;
    //  const debug = (config.debug == 'debug');
     const HighLevelProducer = kafka.HighLevelProducer;
     const Client = kafka.Client;
     const topics = config.topics;
     const debugOn = config.debug;
     const topicArray = topics.split(',');
     const clientWrapper = {};
     clientWrapper.client = new Client(clusterZookeeper);
     clientWrapper.producer = new HighLevelProducer(clientWrapper.client);
     clientWrapper.open = true;
     function debug(...rest) {
       if (debugOn) {
         console.log(...rest);// eslint-disable-line no-console
       }
     }
     // Balance type : one of [
  //    actual: msg.partition is used as partition number for all messages
  //    rr: roundrobin message distribution over all the partitions
  //    id: msg.hashId is used to derive a hash code, and that code is assigned as partition number
  //       is helpful if you want to make sure messages with
  //       one hashId go to same partition. msg.hashId posted as key of message
  //    none: partitioning and everthing else is left to internal implementation
  //       is helpful if you only want a basic HighLevelProducer, without focusing
  //       on partitions. msg.partition and msg.hashId as key are still posted to kafka with message
  //  ]
     const balance = config.balance;

     // optional parameter, will hace comma-seperated entries corresponding to each topic
     const totalPartitions = config.partitions && config.partitions.split(',').map(val => parseInt(val, 10));
     // if partitions are provided, validate them
     if (totalPartitions && totalPartitions.length !== topicArray.length) {
       return node.error('Total topics != Total partition counts \nPartition counts, if provided, should be for each topic, in comma-seperated format');
     }

     // check if a metadata fetch from zookeeper is required or not
     // if totalPartitions are provided, we don't need totalPartitions from metadata
     // if balance is none or actual, no need to maintain totalPartitions at all
     const metadataFetchNotRequired = totalPartitions || balance === 'none' || balance === 'actual';
    //  console.log(`${JSON.stringify(totalPartitions)}.length || ${balance} === none || ${balance} === actual`);

     // balancer is a function which will balance the partitions for messages.
     // Is constructed based on balance type
     node.balancer = null;

     // metadata(partition counts) is stored here
     const topicMetadata = {
     };

     // Is used to store state of producer, will be one of
     // - RoundRobin previous partition number, for each topic
     // - ConsistentHash object for each topic
     const topicDict = {};

     /**
     * @param partitionBuilder - function which creates partition number
     * invoked with (topic)
     * @param key - key assigned to message (see kafka-key)
     * @param msg - the msg object
     */
     function produce(partitionBuilder, key, msg) {
       const payloads = topicArray.map(topic => ({
         topic,
         partition: partitionBuilder(topic),
         key,
         messages: msg.payload,
       }));
       debug(`Sending Message: ${JSON.stringify(payloads)}`);

       clientWrapper.producer.send(payloads, (err, data) => { // eslint-disable-line consistent-return
         if (err) {
           return node.error(err);
         }
         debug(`Kafka success Response ${JSON.stringify(data)}`);
         node.send({
           payload: {
             sent: true,
             msg,
           },
         });
       });
     }

    /**
    * This fuction creates metadata for the balacer.
    * @param callback - is executed when function executes successfully
    **/
     function fetchMetadata(callback) {
       try {
         // If meta data fecth from zk server is realy required or not
         if (metadataFetchNotRequired) {
           // if partition counts are provided
           if (totalPartitions) {
             topicArray.forEach((topic, i) => {
               topicMetadata[topic] = { partitions: totalPartitions[i] || 1 };
             });
             node.status({
               fill: 'green',
               shape: 'dot',
               text: 'metadata not required',
             });
             return callback();
           }
           node.status({
             fill: 'green',
             shape: 'dot',
             text: 'metadata not required',
           });
           callback();
         }

         // start fetch from zk server
         node.status({
           fill: 'grey',
           shape: 'dot',
           text: `fetching metadata from ${clusterZookeeper}`,
         });
         // try fetch
         const loadMetadataForTopics = function fetchMetaRetryWrapper() {
           if (!clientWrapper.open) {
             return;
           }
           this.retries++;
           const thisCtx = this;
           clientWrapper.client.loadMetadataForTopics(topicArray, function(err, metadataResponse){//eslint-disable-line
             if (err) {
               // retry if err
              //  node.error(err);
               debug(`Retrying metadata fetch, retry: ${thisCtx.retries}`);
               if (topicArray.some(topic =>
                  (Object.keys(topicMetadata).indexOf(topic) === -1))) {
                 if (thisCtx.retries % 10 === 0) {
                   clientWrapper.client.close((...rest) => {
                     debug('closed previous connention ', JSON.stringify(rest));
                     clientWrapper.open = false;
                     retryTimeouts.add(() => {
                       clientWrapper.client = new Client(clusterZookeeper);
                       clientWrapper.producer = new HighLevelProducer(clientWrapper.client);
                       clientWrapper.open = true;
                       loadMetadataForTopics();
                     }, 30000);
                   });
                 }
                 retryTimeouts.add(loadMetadataForTopics, 2000);
                 return;
               }
             }
             // console.log('metadata is: ', JSON.stringify(metadataResponse, null, 4));
             try {
               const metadata = metadataResponse[1].metadata;
               Object.keys(metadata).forEach((topicKey) => {
                 const topicMetadataResponse = metadata[topicKey];
                 Object.keys(topicMetadataResponse).forEach((key) => {
                   const topic = topicMetadataResponse[key].topic;
                   const topicData = topicMetadata[topic] = topicMetadata[topic] || { partitions: 0 };
                   topicData.partitions++;
                 });
               });
               node.status({
                 fill: 'green',
                 shape: 'dot',
                 text: `fetched metadata for ${Object.keys(topicMetadata).join(', ')}`,
               });
               callback();
               debug('metadata recieved', JSON.stringify(topicMetadata, null, 2));
             } catch (e) {
               return node.error(e);
             }
           // node.send({ payload: 'blah blah' });
           });
         }.bind({ retries: 0 });
         loadMetadataForTopics();
       } catch (e) {
         node.error(e);
       }
     }
     fetchMetadata(() => {
       try {
         debug('balance type is %s', balance);
         switch (balance) {
           case 'id':
             topicArray.forEach((topic) => {
               const hr = (topicDict[topic] = {
                 hr: new ConsistentHash(),
               }).hr;
               for (let i = 0; i < topicMetadata[topic].partitions; i++) {
                 hr.add(i);
               }
             });
             node.balancer = function idBalancer(msg) {
               if (!msg.hashId) {
                 return node.error('msg.hashId is mandatory in case of consistent-hash');
               }
               const key = msg.hashId;
              //  console.log(topicDict);
               produce(topicKey => topicDict[topicKey].hr.get(key), key, msg);
             };
             break;
           case 'actual':
             node.balancer = function actualBalance(msg) {
               if (!msg.partition) {
                 return node.error('msg.partition is mandatory in case of partition');
               }
               produce(() => msg.partition, undefined, msg);
             };
             break;
           case 'rr':
             topicArray.forEach((topic) => {
               const partitionCount = topicMetadata[topic].partitions;
               topicDict[topic] = {
                 value: -1,
                 next() {
                   return (this.value = (this.value + 1) % partitionCount);
                 },
               };
             });
             node.balancer = function roundrobinBalancer(msg) {
               produce(topicKey => topicDict[topicKey].next(), undefined, msg);
             };
             break;
           default:
             node.balancer = function noneBalancer(msg) {
               produce(() => msg.partition, msg.hashId, msg);
             };
         }
         debug(`node.balancer is ${node.balancer}`);
       } catch (e) {
         return node.error(e);
       }
     });


     this.on('input', (msg) => {
       try {
         if (!node.balancer) {
           node.send({
             payload: false,
           });
           return node.error('Producer Not Ready');
         }
         node.balancer(msg);
       } catch (e) {
         node.error(e);
       }
     });

     node.on('close', () => {
       clientWrapper.client.close();
       [...retryTimeouts].forEach(tid => clearTimeout(tid));
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
     const consumerType = config.consumerType || 'ConsumerGroup';
     const fetchMaxBytes = parseInt(config.fetchMaxBytes, 10);

     const HighLevelConsumer = kafka.HighLevelConsumer;
     const Client = kafka.Client;
     let topics = String(config.topics);
     const clusterZookeeper = config.zkquorum;
     const groupId = config.groupId;
     const debug = (config.debug === 'debug');
     const client = new Client(clusterZookeeper);
     const cgTopics = topics.split(',');
     topics = cgTopics.map(topic => ({
       topic,
     }));

    //  console.log('fetchMaxBytes = ', fetchMaxBytes);
    //  console.log('consumerType =', consumerType);
    //  console.log('debug =', debug);

     if (consumerType === 'high-level-consumer') {
       const options = {
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
             console.log(message); // eslint-disable-line no-console
             node.log(message);
           }
           const msg = { payload: message };
           node.send(msg);
         });

         consumer.on('error', (err) => {
           node.error(err);
         });
       } catch (e) {
         node.error(e);
       }
     } else {
       try {
         const options = {
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
           fetchMaxBytes,
         };
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
