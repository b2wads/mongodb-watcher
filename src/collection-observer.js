const { MongoClient } = require('mongodb')

module.exports = class CollectionObserver {
  constructor({ database, collection, maxEventDuplication, observerId, stateCollection, uri }) {
    this._client = null
    this._changeStream = null
    this._collection = null
    this._collectionName = collection
    this._db = null
    this._dbName = database
    this._eventsCount = 0
    this._handlers = new Map()
    this._mongoUri = uri
    this._stateCollection = null
    this._stateCollectionName = stateCollection
    this._observerId = observerId
    this._saveStateFrequency = maxEventDuplication
    this._saveStateLock = new Promise((resolve) => resolve(true))
  }

  on(operation, callback) {
    this._handlers.set(operation, callback)
    return this
  }

  async _connect() {
    await this._client.connect()

    this._db = this._client.db(this._dbName)

    if (this._stateCollectionName) {
      this._stateCollection = this._db.collection(this._stateCollectionName)

      await this._stateCollection.createIndex('observerId', { unique: true })
    }

    this._collection = this._db.collection(this._collectionName)
  }

  async _getResumeToken() {
    if (!this._stateCollection) return undefined

    const observationState = await this._stateCollection.findOne({ observerId: this._observerId })

    return observationState ? observationState.resumeToken : undefined
  }

  async _saveState(eventData) {
    if (!this._stateCollection) return

    await this._stateCollection.replaceOne(
      { observerId: this._observerId },
      {
        collection: this._collectionName,
        observerId: this._observerId,
        lastObservedId: eventData.documentKey._id,
        resumeToken: eventData._id,
      },
      { upsert: true }
    )
  }

  async _processStream(eventData) {
    await this._saveStateLock

    const operationHandler = this._handlers.get(eventData.operationType)

    if (!operationHandler) return

    this._eventsCount = (this._eventsCount + 1) % this._saveStateFrequency
    operationHandler(eventData)

    if (this._eventsCount === 0) this._saveStateLock = this._saveState(eventData)
  }

  async start() {
    this._client = new MongoClient(this._mongoUri, { useUnifiedTopology: true })
    await this._connect()

    const resumeAfter = await this._getResumeToken()

    // eslint-disable-next-line security/detect-non-literal-fs-filename
    this._changeStream = this._collection.watch({ resumeAfter, fullDocument: 'updateLookup' })

    this._changeStream.on('change', this._processStream.bind(this))
  }

  async stop() {
    if (this._changeStream) await this._changeStream.close()
    if (this._client) await this._client.close()

    this._changeStream = null
    this._client = null
    this._collection = null
    this._db = null
    this._stateCollection = null
  }
}
