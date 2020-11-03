const AsyncPromisePool = require('async-promise-pool')
const yup = require('yup')

const CollectionObserver = require('./collection-observer')
const RabbitPublisher = require('./rabbit-publisher')

const eventTransformers = new Map([['delete', (event) => event.documentKey]])

const defaultTransformer = (event) => event.fullDocument

const constructorValidator = yup.object({
  concurrency: yup.number().min(1),
  mongo: yup.object({
    collection: yup.string().required(),
    database: yup.string().required(),
    operations: yup.array().of(yup.string()).required(),
    stateCollection: yup.string(),
    uri: yup.string().required(),
  }),
  rabbit: yup.object({
    exchange: yup.string().required(),
    routingKey: yup.string(),
    uri: yup.string().required(),
  }),
})

module.exports = class Watcher {
  constructor(args) {
    const {
      concurrency = 10,
      mongo: { collection, database, operations, stateCollection, uri: mongoUri },
      rabbit: { exchange, routingKey, uri: rabbitUri },
    } = constructorValidator.validateSync(args)

    this._observer = new CollectionObserver({
      collection,
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
