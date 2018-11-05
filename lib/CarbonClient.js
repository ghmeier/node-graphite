// @flow
const LazySocket = require('lazy-socket');
const url = require('url');

type Socket = {
  write: (string, string, (err: Error) => void) => void,
  end: () => void
};
type Properties = { dsn: string, socket?: Socket };

class CarbonClient {
  _dsn: string;
  _socket: ?Socket;

  constructor({ dsn, socket }: Properties = {}) {
    this._dsn    = dsn;
    this._socket = socket;
  }

  async write(metrics: Object, timestamp: number): Promise<void> {
    const socket = await this._lazyConnect();

    let lines = '';
    for (const path in metrics) {
      const value = metrics[path];
      lines += [path, value, timestamp].join(' ') + '\n';
    }

    return new Promise((resolve, reject) => {
      socket.write(lines, 'utf-8', (err) => {
        if (err) {
          reject(err);
          return;
        }

        resolve();
      });
    });
  }

  async _lazyConnect(): Promise<Socket> {
    if (this._socket) return this._socket;

    const dsn  = url.parse(this._dsn);
    const port = parseInt(dsn.port, 10) || 2003;
    const host = dsn.hostname;

    this._socket = LazySocket.createConnection(port, host);
    return this._socket;
  }

  end() {
    if (this._socket) this._socket.end();
  }
}

module.exports = CarbonClient;
