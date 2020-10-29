const { ObjectID } = require('mongodb')

module.exports = class CollectionObserver {
  constructor({
    database,
    collection,
    stateCollection,
    mongoClient,
    maxEventDuplication,
  }) {
    this._client = mongoClient
    this._changeStream = null
    this._collection = null
    this._collectionName = collection
    this._db = null
    this._dbName = database
    this._eventsCount = 0
    this._handlers = new Map()
    this._stateCollection = null
    this._stateCollectionName = stateCollection
    this._saveStateFrequency = maxEventDuplication
    this._saveStateLock = new Promise(resolve => resolve(true))
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
      await this._stateCollection.ensureIndex("collection", { unique: true })
    }

    this._collection = this._db.collection(this._collectionName)
  }

  async _getResumeToken() {
    if (!this._stateCollection) return undefined

    const observationState = await this._stateCollection.findOne({ collection: this._collectionName })

    return observationState ? watchMeta.resumeToken : undefined
  }

  async _saveState(eventData) {
    if (!this._stateCollection) return

    await this._stateCollection.replaceOne(
      { collection: this._collectionName },
      {
        collection: this._collectionName,
        lastObservedId: eventData.documentKey._id,
        resumeToken: eventData._id,
      },
      { upsert: true }
    )
  }

  async start() {
    await this._connect()

    const resumeAfter = await this._getResumeToken()

    this._changeStream = this._collection.watch({ resumeAfter })

    this._changeStream.on(
      'change',
      (async eventData => {
        await this._saveStateLock

        const operationHandler = this._handlers.get(eventData.operationType)

        if (!operationHandler) return

        this._eventsCount = (this._eventsCount + 1) % this._saveStateFrequency
        operationHandler(eventData)

        if (this._eventsCount === 0)
          this._saveStateLock = this._saveState(eventData)
      }).bind(this)
    )
  }

  async stop() {
    if (this._changeStream) await this._changeStream.close()
    if (this._client.isConnected()) await this._client.close()

    this._changeStream = null
    this._collection = null
    this._db = null
    this._stateCollection = null
  }
}
