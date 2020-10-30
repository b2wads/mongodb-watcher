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

  describe('when listening only for insert operations', () => {
    const docFixtures = [
      { field1: 'test-1', field2: 'test-2' },
      { field1: 'test-3', field2: 'test-4' },
    ]

    let publishedMsgs
    let savedState
    before(async () => {
      const watcher = new Watcher({
        concurrency: docFixtures.length,
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
      })

      await watcher.start()

      await mongo.collection.insertMany(docFixtures)

      await waitMs(250)

      publishedMsgs = await rabbit.getMessages()

      savedState = await mongo.stateCollection.findOne({ collection: mongo.collectionName })

      await watcher.stop()
    })

    after(async () => {
      await Promise.all([
        mongo.collection.deleteMany({}),
        mongo.stateCollection.deleteMany({}),
        rabbit.channel.purgeQueue(rabbit.queue),
      ])
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
})
