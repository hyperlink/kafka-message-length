'use strict';

const Promise = require('bluebird');
const { KafkaClient, Offset } = require('kafka-node');

Promise.promisifyAll(KafkaClient.prototype);
Promise.promisifyAll(Offset.prototype);

async function fetchNumberOfMessages (topic, kafkaHost) {
  const client = new KafkaClient({ kafkaHost });
  const offset = new Offset(client);
  const topics = [topic];
  const [latest, earliest] = await Promise.join(
    offset.fetchLatestOffsetsAsync(topics),
    offset.fetchEarliestOffsetsAsync(topics)
  );

  let total = 0;
  for (const partition in latest[topic]) {
    total += latest[topic][partition] - earliest[topic][partition];
  }

  const partitions = Object.keys(latest[topic]).length;

  await client.closeAsync();
  return {
    topic,
    total,
    partitions
  };
}

module.exports = fetchNumberOfMessages;
