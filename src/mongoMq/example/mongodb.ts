

import { MongoClient } from 'mongodb';
import MongoMq from '../index';

let url = 'mongodb://localhost:27017/mubsub_example';
let opts = {
  useNewUrlParser: true,
  useUnifiedTopology: true
}
MongoClient.connect(url, opts, function (err, client) {
  let db = client.db();
  let mq = new MongoMq(db, {});
  let channel = mq.channel("test1", { max: 3 });
  channel.subscribe('baz', function (message: any) {
    console.log(message); // => 'baz'
  });
});
