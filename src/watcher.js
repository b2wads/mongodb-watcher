// TODO move this to separate NPM package

const { MongoClient, ObjectID } = require('mongodb')
const amqplib = require('amqplib')
const AsyncPromisePool = require('async-promise-pool')

module.exports = class Watcher {
  constructor({
    concurrency,
    operations,
    mongo: {
      uri: mongoUri,
      database,
      collection,
      metaCollection,
      options,
    },
    rabbit: {
      uri: rabbitUri,
      exchange,
      routingKey,
    },
  }) {
    this._operations = new Set(operations)
    this._maxNotificationDuplicates = concurrency

    this._mongo = {
      changeStream: false,
      client: new MongoClient(mongoUri, options),
      collection: false,
      collectionName: collection,
      db: false,
      dbName: database,
      metaCollection: false,
      metaCollectionName: metaCollection,
    }

    this._notificationCounter = 0

    this._pool = new AsyncPromisePool({ concurrency })

    this._rabbit = {
      uri: rabbitUri,
      connection: false,
      channel: false,
      exchange,
      routingKey,
    }
  }

  async _initCollections() {
    await this._mongo.client.connect()

    this._mongo.db = this._mongo.client.db(this._mongo.dbName)

    if (this._mongo.metaCollectionName) {
      this._mongo.metaCollection = this._mongo.db.collection(this._mongo.metaCollectionName)
      await this._mongo.metaCollection.ensureIndex("collection", { unique: true })
    }

    this._mongo.collection = this._mongo.db.collection(this._mongo.collectionName)
  }

  async _getLastWatchedId() {
    if (!this._mongo.metaCollection) return undefined

    const watchMeta = await this._mongo.metaCollection.findOne({ collection: this._mongo.collectionName })

    return watchMeta.lastWatchedId
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

  async start() {
    await Promise.all([
      this._initCollections(),
      this._initRabbit()
    ])

    const resumeAfter = await this._getLastWatchedId()

    const rabbit = this._rabbit

    this._mongo.changeStream = this._mongo.collection.watch({ resumeAfter })

    this._mongo.changeStream.on('change', (eventData) => {
      if (!this._operations.has(eventData.operationType)) return

      this._notificationCounter = (this._notificationCounter + 1) % this._maxNotificationDuplicates

      this._pool.add(() => rabbit.channel.publish(rabbit.exchange, rabbit.routingKey, Buffer.from(JSON.stringify(eventData.fullDocument))))

      if (this._notificationCounter === 0) {
        // await this._updateLastWatchedId(eventData._id)
      }
    })
  }

  async stop() {
    // const promises = [
    //   this._rabbit.connection.close(),
    // ]
    const promises = []

    if (this._mongo.db) {
      promises.push(
        this._mongo.changeStream
          .close()
          .then(() => this._mongo.client.close())
      )

      this._mongo.db = false
      this._mongo.collection = false
      this._mongo.metaCollection = false
    }

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

