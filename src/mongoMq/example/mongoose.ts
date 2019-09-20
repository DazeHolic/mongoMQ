
import MongoMq from '../index';
import mongoose from 'mongoose';


let url = 'mongodb://localhost:27017/mubsub_example';
let opts = {
  useNewUrlParser: true,
  useUnifiedTopology: true
}
let connect = mongoose.createConnection(url, opts);
connect.on('connected', function (err) {
  let mq = new MongoMq(connect, {});
  let channel = mq.channel("test1", { max: 3 });
  channel.subscribe('baz', function (message: any) {
    console.log(message);
  });
  channel.publish('baz', "baztest");
});

