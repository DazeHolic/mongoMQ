import { EventEmitter } from 'events';
import * as util from 'util';
import Channel from './channel';

export default class Connection {
  private _db: any;
  private _destroyed: boolean;
  private _channels: any;
  private _options: any;

  // support mongoose and mongodb
  constructor(db: any, options: any) {
    this._db = db;
    this._options = options;
    this._destroyed = false;
    this._channels = {};
  }

  /**
   * Current connection state.
   *
   * @type {String}
   * @api public
   */
  state() {
    if (this._destroyed) {
      return 'destroyed';
    } else {
      return 'connecting';
    }
  }

  /**
   * Creates or returns a channel with the passed name.
   *
   * @see Channel
   * @return {Channel}
   * @api public
   */ 
  channel(name: string, options: any) {
    if (typeof name === 'object') {
      options = name;
      name = 'noName';
    }
    if (!this._channels[name] || this._channels[name].closed) {
      this._channels[name] = new Channel(this, name, options);
    }
    return this._channels[name];
  }

  /**
  * Close connection.
  */
  close() {
    this._destroyed = true;
  }
}
