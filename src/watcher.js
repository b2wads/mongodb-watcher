const AsyncPromisePool = require('async-promise-pool')
const { MongoClient } = require('mongodb')

const CollectionObserver = require('./collection-observer')
const RabbitPublisher = require('./rabbit-publisher')

module.exports = class Watcher {
  constructor({
    concurrency,
    mongo: { database, collection, connectionOptions, stateCollection, operations, uri: mongoUri },
    rabbit: { uri: rabbitUri, exchange, routingKey },
  }) {
    this._operations = new Set(operations)

    this._observer = new CollectionObserver({
      collection,
      database,
      stateCollection,
      // FIXME connecting to mongo with useUnifiedTopology: true breaks the changeStream
      mongoClient: new MongoClient(mongoUri, connectionOptions),
      operations,
      maxEventDuplication: concurrency,
    })

    this._pool = new AsyncPromisePool({ concurrency })

    this._publisher = new RabbitPublisher({
      exchange,
      routingKey,
      uri: rabbitUri,
    })

    const operationHandler = this._handleEvent.bind(this)
    operations.forEach((operation) => {
      this._observer.on(operation, operationHandler)
    })
  }

  _handleEvent(eventData) {
    this._pool.add(() => this._publisher.publishObject(eventData.fullDocument))
  }

  async start() {
    await this._publisher.connect()
    await this._observer.start()
  }

  async stop() {
    await this._observer.stop()
    await this._publisher.close()
  }
}
