const { ObjectID } = require('mongodb')

module.exports = class CollectionObserver {
  constructor({
    database,
    collection,
    metaCollection,
    mongoClient,
  }) {
    this._client = mongoClient
    this._changeStream = null
    this._collection = null
    this._collectionName = collection
    this._db = null
    this._dbName = database
    this._handlers = new Map()
    this._metaCollection = null
    this._metaCollectionName = metaCollection
  }

  on(operation, callback) {
    this._handlers.set(operation, callback)
    return this
  }

  async _connect() {
    await this._client.connect()

    this._db = this._client.db(this._dbName)

    if (this._metaCollectionName) {
      this._metaCollection = this._db.collection(this._metaCollectionName)
      await this._metaCollection.ensureIndex("collection", { unique: true })
    }

    this._collection = this._db.collection(this._collectionName)
  }

  async _getLastWatchedId() {
    if (!this._metaCollection) return undefined

    const watchMeta = await this._metaCollection.findOne({ collection: this._collectionName })

    return watchMeta.lastWatchedId
  }

  async start() {
    await this._connect()

    const resumeAfter = await this._getLastWatchedId()

    this._changeStream = this._collection.watch({ resumeAfter })

    this._changeStream.on(
      'change',
      (eventData => {
        const operationHandler = this._handlers.get(eventData.operationType)

        if (!operationHandler) return

        operationHandler(eventData)
      }).bind(this)
    )
  }

  async stop() {
    if (this._changeStream) await this._changeStream.close()
    if (this._client.isConnected()) await this._client.close()

    this._changeStream = null
    this._collection = null
    this._db = null
    this._metaCollection = null
  }
}
