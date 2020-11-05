/* eslint-disable no-new */

const { expect } = require('chai')

const { Watcher } = require('../../src')

describe('[UNIT] watcher', () => {
  describe('when instantiating watcher with incorrect configuration', () => {
    const baseConfigFixture = {
      concurrency: 10,
      mongo: {
        collection: 'testcollection',
        database: 'testdatabase',
        observerId: 'testobserverid',
        operations: ['insert'],
        stateCollection: 'teststatecollection',
        uri: 'mongodb://localhost',
      },
      rabbit: {
        exchange: 'testexchange',
        uri: 'amqp://localhost',
      },
    }

    describe('missing mongo collection', () => {
      const configFixture = {
        ...baseConfigFixture,
        mongo: { ...baseConfigFixture.mongo },
      }
      delete configFixture.mongo.collection

      let thrownError

      before(() => {
        try {
          new Watcher(configFixture)
        } catch (err) {
          thrownError = err
        }
      })

      it('should throw an error', () => {
        expect(thrownError).to.exist
      })

      it('should throw error describing the problem', () => {
        expect(thrownError.message).to.match(/collection .* required/i)
      })
    })

    describe('missing mongo database', () => {
      const configFixture = {
        ...baseConfigFixture,
        mongo: { ...baseConfigFixture.mongo },
      }
      delete configFixture.mongo.database

      let thrownError

      before(() => {
        try {
          new Watcher(configFixture)
        } catch (err) {
          thrownError = err
        }
      })

      it('should throw an error', () => {
        expect(thrownError).to.exist
      })

      it('should throw error describing the problem', () => {
        expect(thrownError.message).to.match(/database .* required/i)
      })
    })

    describe('missing mongo operations', () => {
      const configFixture = {
        ...baseConfigFixture,
        mongo: {
          ...baseConfigFixture.mongo,
          operations: [],
        },
      }

      let thrownError

      before(() => {
        try {
          new Watcher(configFixture)
        } catch (err) {
          thrownError = err
        }
      })

      it('should throw an error', () => {
        expect(thrownError).to.exist
      })

      it('should throw error describing the problem', () => {
        expect(thrownError.message).to.match(/operations .* required/i)
      })
    })

    describe('missing mongo uri', () => {
      const configFixture = {
        ...baseConfigFixture,
        mongo: { ...baseConfigFixture.mongo },
      }
      delete configFixture.mongo.uri

      let thrownError

      before(() => {
        try {
          new Watcher(configFixture)
        } catch (err) {
          thrownError = err
        }
      })

      it('should throw an error', () => {
        expect(thrownError).to.exist
      })

      it('should throw error describing the problem', () => {
        expect(thrownError.message).to.match(/mongo\.uri .* required/i)
      })
    })

    describe('missing rabbit exchange', () => {
      const configFixture = {
        ...baseConfigFixture,
        rabbit: { ...baseConfigFixture.rabbit },
      }
      delete configFixture.rabbit.exchange

      let thrownError

      before(() => {
        try {
          new Watcher(configFixture)
        } catch (err) {
          thrownError = err
        }
      })

      it('should throw an error', () => {
        expect(thrownError).to.exist
      })

      it('should throw error describing the problem', () => {
        expect(thrownError.message).to.match(/exchange .* required/i)
      })
    })

    describe('missing rabbit uri', () => {
      const configFixture = {
        ...baseConfigFixture,
        rabbit: { ...baseConfigFixture.rabbit },
      }
      delete configFixture.rabbit.uri

      let thrownError

      before(() => {
        try {
          new Watcher(configFixture)
        } catch (err) {
          thrownError = err
        }
      })

      it('should throw an error', () => {
        expect(thrownError).to.exist
      })

      it('should throw error describing the problem', () => {
        expect(thrownError.message).to.match(/rabbit\.uri .* required/i)
      })
    })

    describe('specifying state collection without an observer id', () => {
      const configFixture = {
        ...baseConfigFixture,
        mongo: { ...baseConfigFixture.mongo },
      }
      delete configFixture.mongo.observerId

      let thrownError

      before(() => {
        try {
          new Watcher(configFixture)
        } catch (err) {
          thrownError = err
        }
      })

      it('should throw an error', () => {
        expect(thrownError).to.exist
      })

      it('should throw error describing the problem', () => {
        expect(thrownError.message).to.match(/cannot specify state collection without an observer id/i)
      })
    })
  })
})
