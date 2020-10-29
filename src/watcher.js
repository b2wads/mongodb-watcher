// TODO move this to separate NPM package

const amqplib = require('amqplib')
const AsyncPromisePool = require('async-promise-pool')
const { MongoClient } = require('mongodb')

const CollectionObserver = require('./collection-observer')

module.exports = class Watcher {
  constructor({
    concurrency,
    mongo: {
      database,
      collection,
      connectionOptions,
      metaCollection,
      operations,
      uri: mongoUri,
    },
    rabbit: {
      uri: rabbitUri,
      exchange,
      routingKey,
    },
  }) {
    this._operations = new Set(operations)
    this._maxNotificationDuplicates = concurrency

    this._observer = new CollectionObserver({
      collection,
      database,
      metaCollection,
      // FIXME connecting to mongo with useUnifiedTopology: true breaks the changeStream
      mongoClient: new MongoClient(mongoUri, connectionOptions),
      operations,
    })

    this._notificationCounter = 0

    this._pool = new AsyncPromisePool({ concurrency })

    this._rabbit = {
      uri: rabbitUri,
      connection: false,
      channel: false,
      exchange,
      routingKey,
    }

    const operationHandler = this._handleEvent.bind(this)
    operations.forEach(operation => {
      this._observer.on(operation, operationHandler)
    })
  }

  async _updateLastWatchedId(id) {
    if (!this._mongo.metaCollection) return

    await this._mongo.metaCollection.replaceOne(
      { collection: this._mongo.collectionName },
      {
        collection: this._mongo.collectionName,
        lastWatchedId: id,
      },
      { upsert: true }
    )
  }

  async _initRabbit() {
    this._rabbit.connection = await amqplib.connect(this._rabbit.uri)
    this._rabbit.channel = await this._rabbit.connection.createChannel()
  }

  _handleEvent(eventData) {
    this._notificationCounter = (this._notificationCounter + 1) % this._maxNotificationDuplicates

    this._pool.add(() =>
      this._rabbit.channel.publish(
        this._rabbit.exchange,
        this._rabbit.routingKey,
        Buffer.from(JSON.stringify(eventData.fullDocument))
      )
    )

    if (this._notificationCounter === 0) {
      // await this._updateLastWatchedId(eventData._id)
    }
  }

  async start() {
    await this._initRabbit()
    await this._observer.start()
  }

  async stop() {
    const promises = [
      this._observer.stop()
    ]

    if (this._rabbit.channel) {
      promises.push(
        this._rabbit.channel
          .close()
          .then(() => this._rabbit.connection.close())
      )
    }

    return Promise.all(promises)
  }
}

