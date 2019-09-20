import { EventEmitter } from 'events';
import * as util from 'util';

let noop = function () { };

export default class Channel extends EventEmitter {
  private _options: any;
  private _connection: any;
  private _closed: boolean;
  private _listening: any;
  private _name: string;
  private _collection: any;


  /**
   * Channel constructor.
   *
   * @param {Connection} connection
   * @param {String} [name] optional channel/collection name, default is 'mubsub'
   * @param {Object} [options] optional options
   *   - `size` max size of the collection in bytes, default is 5mb
   *   - `max` max amount of documents in the collection
   *   - `retryInterval` time in ms to wait if no docs found, default is 200ms
   *   - `recreate` recreate the tailable cursor on error, default is true
   */
  constructor(connection: any, name: string, options: any) {
    super();
    options || (options = {});
    options.capped = true;
    // In mongo v <= 2.2 index for _id is not done by default
    options.autoIndexId = true;
    options.size || (options.size = 1024 * 1024 * 5);
    options.strict = false;

    this._options = options;
    this._connection = connection;
    this._closed = false;
    this._listening = null;
    this._name = name || 'mubsub';
    this._collection = null;

    this.create().listen();
    this.setMaxListeners(0);
  }


  /**
   * Create a channel collection.
   *
   * @return {Channel} this
   * @api private
   */
  private create() {
    let self = this;
    self._connection._db.createCollection(
      self._name,
      self._options,
      function (err: Error, collection: any) {
        if (err && err.message === 'collection already exists') {
          return self.create();
        } else if (err) {
          return self.emit('error', err);
        }
        self.emit('collection', self._collection = collection);
      }
    );
    return this;
  };


  /**
   * Create a listener which will emit events for subscribers.
   * It will listen to any document with event property.
   *
   * @param {Object} [latest] latest document to start listening from
   * @return {Channel} this
   * @api private
   */
  private listen(latest?: any) {
    let self = this;
    this.latest(latest, this.handle(true, function (latest: any, collection: any) {
      var cursor = collection
        .find(
          { _id: { $gt: latest._id } },
          {
            tailable: true,
            awaitData: true,
            timeout: false,
            sortValue: { $natural: -1 },
            numberOfRetries: Number.MAX_VALUE,
            tailableRetryInterval: self._options.retryInterval
          }
        );

      var next = self.handle(false, function (doc: any) {
        // There is no document only if the cursor is closed by accident.
        // F.e. if collection was dropped or connection died.
        if (!doc) {
          return setTimeout(function () {
            self.emit('error', new Error('Mubsub: broken cursor.'));
            if (self._options.recreate) {
              self.create().listen(latest);
            }
          }, 1000);
        }

        latest = doc;

        if (doc.event) {
          self.emit(doc.event, doc.message);
          self.emit('message', doc.message);
        }
        self.emit('document', doc);
        process.nextTick(more);
      });

      var more = function () {
        cursor.next(next);
      };

      more();
      self._listening = collection;
      self.emit('ready', collection);
    }));

    return this;
  };

  /**
   * Close the channel.
   *
   * @return {Channel} this
   * @api public
   */
  close() {
    this._closed = true;
    return this;
  };

  /**
   * Publish an event.
   *
   * @param {String} event
   * @param {Object} [message]
   * @param {Function} [callback]
   * @return {Channel} this
   * @api public
   */
  publish(event: string, message: any, callback: Function) {
    var options = callback ? { safe: true } : {};
    callback || (callback = noop);

    this.ready(function (collection: any) {
      collection.insertOne({ event: event, message: message }, options, function (err: Error, docs: any) {
        if (err) return callback(err);
        callback(null, docs.ops[0]);
      });
    });
    return this;
  };


  /**
   * Subscribe an event.
   *
   * @param {String} [event] if no event passed - all events are subscribed.
   * @param {Function} callback
   * @return {Object} unsubscribe function
   * @api public
   */
  subscribe(event: string, callback: any) {
    var self = this;
    if (typeof event == 'function') {
      callback = event;
      event = 'message';
    }
    this.on(event, callback);
    return {
      unsubscribe: function () {
        self.removeListener(event, callback);
      }
    };
  };


  /**
   * Get the latest document from the collection. Insert a dummy object in case
   * the collection is empty, because otherwise we don't get a tailable cursor
   * and need to poll in a loop.
   *
   * @param {Object} [latest] latest known document
   * @param {Function} callback
   * @return {Channel} this
   * @api private
   */
  private latest(latest: any, callback: Function) {
    function onCollection(collection: any) {
      collection
        .find(latest ? { _id: latest._id } : null, { timeout: false })
        .sort({ $natural: -1 })
        .limit(1)
        .next(function (err: Error, doc: any) {
          if (err || doc) return callback(err, doc, collection);

          collection.insertOne({ 'dummy': true }, { safe: true }, function (err: Error, docs: any) {
            if (err) return callback(err);
            callback(err, docs.ops[0], collection);
          });
        });
    }

    this._collection ? onCollection(this._collection) : this.once('collection', onCollection);
    return this;
  };

  /**
   * Return a function which will handle errors and consider channel and connection
   * state.
   *
   * @param {Boolean} [exit] if error happens and exit is true, callback will not be called
   * @param {Function} callback
   * @return {Function}
   * @api private
   */
  private handle(exit: boolean, callback: Function) {
    var self = this;
    if (typeof exit === 'function') {
      callback = exit;
      exit = false;
    }
    return function () {
      if (self._closed || self._connection.destroyed) return;

      var args = [].slice.call(arguments);
      var err = args.shift();

      if (err) self.emit('error', err);
      if (err && exit) return;

      callback.apply(self, args);
    };
  };

  /**
   * Call back if collection is ready for publishing.
   *
   * @param {Function} callback
   * @return {Channel} this
   * @api private
   */
  private ready(callback: any) {
    if (this._listening) {
      callback(this._listening);
    } else {
      this.once('ready', callback);
    }

    return this;
  };


}








