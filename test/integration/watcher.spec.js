const amqplib = require('amqplib')
const { expect } = require('chai')
const { MongoClient } = require('mongodb')

const { Watcher } = require('../../index')

const waitMs = require('../helpers/wait-ms')

describe('[INTEGRATION] watcher', () => {
  const rabbit = {
    uri: process.env.RABBITMQ_URI || 'amqp://localhost:5672',
    queue: 'test_queue',
    exchange: 'test_exchange',

    async getMessages() {
      const msgs = []
      let msg

      // eslint-disable-next-line no-cond-assign
      while ((msg = await this.channel.get(this.queue, { noAck: true }))) msgs.push(JSON.parse(msg.content.toString()))

      return msgs
    },
  }

  const mongo = {
    uri: process.env.MONGODB_URI || 'mongodb://localhost:27017/?replicaSet=testReplSet',
    dbName: 'testdatabase',
    collectionName: 'testcollection',
    stateCollectionName: 'observationstates',
  }

  const defaultWatcherConfig = {
    concurrency: 1,
    mongo: {
      uri: mongo.uri,
      database: mongo.dbName,
      collection: mongo.collectionName,
      stateCollection: mongo.stateCollectionName,
      operations: ['insert'],
    },
    rabbit: {
      uri: rabbit.uri,
      exchange: rabbit.exchange,
    },
  }

  before(async () => {
    rabbit.conn = await amqplib.connect(rabbit.uri)
    rabbit.channel = await rabbit.conn.createChannel()

    await Promise.all([
      rabbit.channel.assertQueue(rabbit.queue),
      rabbit.channel.assertExchange(rabbit.exchange, 'fanout'),
    ])

    await rabbit.channel.bindQueue(rabbit.queue, rabbit.exchange)

    mongo.client = new MongoClient(mongo.uri, { useUnifiedTopology: true })
    await mongo.client.connect()

    mongo.db = mongo.client.db(mongo.dbName)
    mongo.collection = mongo.db.collection(mongo.collectionName)
    mongo.stateCollection = mongo.db.collection(mongo.stateCollectionName)
  })

  after(async () => {
    await rabbit.channel.unbindQueue(rabbit.queue, rabbit.exchange)

    await Promise.all([rabbit.channel.deleteQueue(rabbit.queue), rabbit.channel.deleteExchange(rabbit.exchange)])

    await rabbit.channel.close()
    await rabbit.conn.close()

    await mongo.client.close()
  })

  afterEach(async () => {
    await Promise.all([
      mongo.collection.deleteMany({}),
      mongo.stateCollection.deleteMany({}),
      rabbit.channel.purgeQueue(rabbit.queue),
    ])
  })

  describe('when listening only for insert operations', () => {
    const docFixtures = [
      { field1: 'test-1', field2: 'test-2' },
      { field1: 'test-3', field2: 'test-4' },
    ]

    let publishedMsgs
    let savedState
    before(async () => {
      const watcher = new Watcher({
        ...defaultWatcherConfig,
        concurrency: docFixtures.length,
      })

      await watcher.start()

      await mongo.collection.insertMany(docFixtures)

      await waitMs(250)

      publishedMsgs = await rabbit.getMessages()

      savedState = await mongo.stateCollection.findOne({ collection: mongo.collectionName })

      await watcher.stop()
    })

    it('should correctly publish all events to rabbit', () => {
      expect(publishedMsgs).to.have.lengthOf(docFixtures.length)
    })

    it('should publish correct information to rabbit', () => {
      expect(publishedMsgs).to.have.deep.members(
        docFixtures.map((doc) => ({
          _id: doc._id.toHexString(),
          field1: doc.field1,
          field2: doc.field2,
        }))
      )
    })

    it('should correctly save observation state', () => {
      expect(savedState).to.exist
      expect(savedState).to.have.property('lastObservedId')
      expect(savedState).to.have.property('resumeToken')
    })
  })

  describe('when listening only for update operations', () => {
    const originalFixture = {
      field1: 'test-1',
      field2: 'test-2',
    }

    const updateFixture = {
      field2: 'test-3',
    }

    let publishedMsgs
    let savedState

    before(async () => {
      const watcher = new Watcher({
        ...defaultWatcherConfig,
        mongo: {
          ...defaultWatcherConfig.mongo,
          operations: ['update'],
        },
      })

      await watcher.start()

      await mongo.collection.insertOne(originalFixture)

      await mongo.collection.findOneAndUpdate({ _id: originalFixture._id }, { $set: updateFixture })

      await waitMs(250)

      publishedMsgs = await rabbit.getMessages()

      savedState = await mongo.stateCollection.findOne({ collection: mongo.collectionName })

      await watcher.stop()
    })

    it('should only publish update event', () => {
      expect(publishedMsgs).to.have.lengthOf(1)
    })

    it('should publish updated document to rabbit', () => {
      expect(publishedMsgs).to.deep.equal([
        {
          ...originalFixture,
          ...updateFixture,
          _id: originalFixture._id.toHexString(),
        },
      ])
    })

    it('should correctly save observation state', () => {
      expect(savedState).to.exist
      expect(savedState).to.have.property('lastObservedId')
      expect(savedState.lastObservedId.toHexString()).to.be.equal(originalFixture._id.toHexString())
      expect(savedState).to.have.property('resumeToken')
    })
  })

  describe('when listening only for delete operations', () => {
    const docFixture = { field1: 'test-1', field2: 'test-2' }

    let publishedMsgs
    let savedState

    before(async () => {
      const watcher = new Watcher({
        ...defaultWatcherConfig,
        mongo: {
          ...defaultWatcherConfig.mongo,
          operations: ['delete'],
        },
      })

      await watcher.start()

      await mongo.collection.insertOne(docFixture)

      await mongo.collection.deleteOne({ _id: docFixture._id })

      await waitMs(250)

      publishedMsgs = await rabbit.getMessages()

      savedState = await mongo.stateCollection.findOne({ collection: mongo.collectionName })

      await watcher.stop()
    })

    it('should correctly publish event to rabbit', () => {
      expect(publishedMsgs).to.have.lengthOf(1)
    })

    it('should publish correct data to rabbit', () => {
      expect(publishedMsgs).to.deep.equal([
        {
          _id: docFixture._id.toHexString(),
        },
      ])
    })

    it('should correctly save observation state', () => {
      expect(savedState).to.exist
      expect(savedState).to.have.property('lastObservedId')
      expect(savedState.lastObservedId.toHexString()).to.be.equal(docFixture._id.toHexString())
      expect(savedState).to.have.property('resumeToken')
    })
  })

  describe('when resuming watch after a failure', () => {
    const docFixtures = [
      { field1: 'test-1', field2: 'test-2' },
      { field1: 'test-3', field2: 'test-4' },
      { field1: 'test-5', field2: 'test-6' },
      { field1: 'test-6', field2: 'test-7' },
    ]

    let publishedMsgs

    before(async () => {
      const watcher = new Watcher({
        ...defaultWatcherConfig,
        concurrency: 2,
      })

      await watcher.start()

      const firstDocs = docFixtures.slice(0, 2)
      await mongo.collection.insertMany(firstDocs)

      await waitMs(250)
      // simulate failure
      await watcher.stop()

      // documents inserted while watcher is down
      const lastDocs = docFixtures.slice(2, docFixtures.length)
      await mongo.collection.insertMany(lastDocs)

      // watcher resumes
      await watcher.start()
      await waitMs(250)
      await watcher.stop()

      publishedMsgs = await rabbit.getMessages()
    })

    it('should not miss any events', () => {
      expect(publishedMsgs).to.have.lengthOf(docFixtures.length)
    })

    it('should publish correct information to rabbit', () => {
      expect(publishedMsgs).to.have.deep.members(
        docFixtures.map((doc) => ({
          _id: doc._id.toHexString(),
          field1: doc.field1,
          field2: doc.field2,
        }))
      )
    })
  })

  // TODO test replace operation

  // TODO test invalidate operation

  describe('when starting watcher for the first time after events have already occurred', () => {
    const firstDocs = [
      { field1: 'test-1', field2: 'test-2' },
      { field1: 'test-3', field2: 'test-4' },
    ]

    const lastDocs = [
      { field1: 'test-5', field2: 'test-6' },
      { field1: 'test-6', field2: 'test-7' },
    ]

    let publishedMsgs

    before(async () => {
      const watcher = new Watcher({
        ...defaultWatcherConfig,
        concurrency: 2,
      })

      // documents inserted while watcher has not started
      await mongo.collection.insertMany(firstDocs)

      await watcher.start()

      await mongo.collection.insertMany(lastDocs)

      await waitMs(250)
      await watcher.stop()

      publishedMsgs = await rabbit.getMessages()
    })

    it('should ignore initial events', () => {
      expect(publishedMsgs).to.have.lengthOf(lastDocs.length)
    })

    it('should publish correct information to rabbit', () => {
      expect(publishedMsgs).to.have.deep.members(
        lastDocs.map((doc) => ({
          _id: doc._id.toHexString(),
          field1: doc.field1,
          field2: doc.field2,
        }))
      )
    })
  })
})
