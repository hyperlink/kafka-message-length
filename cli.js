#!/usr/bin/env node

'use strict';

const fetchNumberOfMessages = require('./index');
const program = require('commander');
const humanize = require('humanize-number');

program
  .version(require('./package.json').version)
  .option('-b, --bootstrap-server <kafka brokers>', 'kafka server to connect to')
  .option('-t, --topic <topic>', 'Topic')
  .parse(process.argv);

if (program.topic && program.bootstrapServer) {
  fetchNumberOfMessages(program.topic, program.bootstrapServer)
    .then(v =>
      console.log(
        `Topic ${v.topic} has ${humanize(v.partitions)} partitions with a total of ${humanize(v.total)} messages.`
      )
    )
    .catch(e => console.error('error', e));
} else {
  console.error('--topic and --boostrap-server args are required.');
}
