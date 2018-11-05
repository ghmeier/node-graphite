// @flow
const CarbonClient = require('./CarbonClient');

type Properties = { carbon: CarbonClient };

class GraphiteClient {
  _carbon: CarbonClient;

  constructor({ carbon }: Properties = {}) {
    this._carbon = carbon;
  }

  static createClient(carbonDsn: string): GraphiteClient {
    const client = new GraphiteClient({
      carbon: new CarbonClient({dsn: carbonDsn}),
    });
    return client;
  }

  static flatten(obj: Object, flat: Object = {}, prefix: string = ''): Object {
    for (const key in obj) {
      const value = obj[key];
      if (typeof value === 'object') {
        GraphiteClient.flatten(value, flat, prefix + key + '.');
      } else {
        flat[prefix + key] = value;
      }
    }

    return flat;
  }

  static appendTags(flatMetrics: Object, tags: Object): Object {
    let tagSuffix = '';
    const res = {};

    const flatTags = GraphiteClient.flatten(tags);
    for (const key in flatTags) {
      tagSuffix += ';' + key + '=' + flatTags[key];
    }

    for (const key in flatMetrics) {
      res[key + tagSuffix] = flatMetrics[key];
    }

    return res;
  }

  /**
   * Writes the given metrics to the underlying plaintext socket to Graphite
   *
   * If no timestamp is given, the current Unix epoch is used (second precision).
   *
   * If a timestamp is provided, it must have a millisecond precision, otherwise
   * Graphite will probably reject the data.
   *
   * @param {object} metrics
   * @param {object} timestamp
   */
  async write(metrics: Object, timestamp: number = Date.now()): Promise<void> {
    // cutting timestamp for precision up to the second
    timestamp = Math.floor(timestamp / 1000);

    return this._carbon.write(GraphiteClient.flatten(metrics), timestamp);
  }

  async writeTagged(metrics: Object, tags: Object, timestamp: number): Promise<void> {
    const taggedMetrics = GraphiteClient.appendTags(GraphiteClient.flatten(metrics), tags);
    return this.write(taggedMetrics, timestamp);
  }

  end() {
    this._carbon.end();
  }
}

module.exports = GraphiteClient;
