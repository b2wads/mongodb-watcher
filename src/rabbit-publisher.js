const amqplib = require('amqplib')

module.exports = class RabbitPublisher {
  constructor({
    exchange,
    routingKey,
    uri,
  }) {
    this._channel = null
    this._connection = null
    this._exchange = exchange
    this._routingKey = routingKey
    this._uri = uri
  }

  async connect() {
    this._connection = await amqplib.connect(this._uri)
    this._channel = await this._connection.createChannel()
  }

  async close() {
    if (this._channel) await this._channel.close()
    if (this._connection) await this._connection.close()

    this._channel = null
    this._connection = null
  }

  async publishObject(obj) {
    return this._channel.publish(this._exchange, this._routingKey, Buffer.from(JSON.stringify(obj)))
  }
}

