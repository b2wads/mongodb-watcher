const AsyncPromisePool = require('async-promise-pool')

const CollectionObserver = require('./collection-observer')
const RabbitPublisher = require('./rabbit-publisher')

const eventTransformers = new Map([['delete', (event) => event.documentKey]])

const defaultTransformer = (event) => event.fullDocument

module.exports = class Watcher {
  constructor({
    concurrency,
    mongo: { database, collection, connectionOptions, stateCollection, operations, uri: mongoUri },
    rabbit: { uri: rabbitUri, exchange, routingKey },
  }) {
    this._operations = new Set(operations)

    this._observer = new CollectionObserver({
      collection,
      connectionOptions,
      database,
      stateCollection,
      operations,
      maxEventDuplication: concurrency,
      uri: mongoUri,
    })

    this._pool = new AsyncPromisePool({ concurrency })

    this._publisher = new RabbitPublisher({
      exchange,
      routingKey,
      uri: rabbitUri,
    })

    const operationHandler = this._handleEvent.bind(this)
    operations.forEach((operation) => {
      const transform = eventTransformers.get(operation) || defaultTransformer

      this._observer.on(operation, (eventData) => operationHandler(transform(eventData)))
    })
  }

  _handleEvent(transformedEvent) {
    this._pool.add(() => this._publisher.publishObject(transformedEvent))
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
