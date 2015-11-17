var expect = require('chai').expect
var DB = require('../db')
// var zonefile = require('../test-zones/com.zone')
var spawn = require('child_process').spawn
var path = require('path')
var es = require('event-stream')
var config = require('../config')
var ZoneImporter = require('../zone-importer')
var logger = require('../logger')('TEST')

var mongo
var db


function dbClean(done) {
  db.db.open(function () {
    db.db.dropCollection('com', function (err) {
      if(err && (err.message).match(/ns not found/)) {
        return done(); // Ignore 'ns not found' error if collection did not exist
      }
      done(err)
    })

  })
}

function dbSetup(done) {
  mongo = spawn('mongod', ['--dbpath=' + config._DBPATH])

  mongo.once('error', done)
  mongo
  .stdout
  .pipe(es.split())
  .pipe(es.map(function (line, cb) {
    if(/(error)/.test(line)) {
      logger.log('line', line)
      throw new Error('MONGO: ' + line)
      cb(null, line)
    }
  }))

  setTimeout(function () {
    mongo.removeListener('error', done)
    done()
  },1000)
}

function dbKill(done) {
  logger.log('Killing server ---------------')
  mongo.once('close', end)
  mongo.kill()
  function end() {
    logger.log('Mongo killed --------------')
    done()
  }
}

describe('Zonefile importer', function(){

  before(function(done){
    config.reset({
      LOGEVERYXLINES: 1,
      ZONEFILE: path.resolve(__dirname, '../test-zones/com.zone'),
      DB: 'test',
      WRITECONCERN: 1
    })
    logger.log('Opening DB -------------')
    logger.log('..on: ', config._DBPATH)
    logger.log('..collection: ', config.COLLECTION)
    dbSetup(function () {
      db = DB()
      db.connect(done)
    })
  })

  after(function(done){
    this.timeout(10 * 1000)
    logger.log('Closing connection...')
    db.close(function () {
      logger.log('Closed connection')
      dbKill(done)
    })
  })

  beforeEach(function(done){
    db.db.open(function () {
      dbClean(done)
    })
  })

  it('there should not be any records', function(done){
    this.timeout(5 * 1000)

    var res = db.db.collection(config.COLLECTION).find({}).count()
    res.then(function (count) {
      expect(count).to.equal(0)
      done()
    }).catch(done)
    
  })

  it('should not accept duplicate record', function(done){
    this.timeout(5 * 1000)
    var zi = ZoneImporter({verbose: true})
    zi.setupDB(function () {
      zi.db.insert({domain: 'josh'}, function (err) {
        expect(err).to.equal(null)
        zi.db.insert({domain: 'josh'}, function (err) {
          expect(err.message).to.match(/duplicate/)
          done()
        })
      })
    })

  })

  it('should insert NS records and record stats', function(done){

    this.timeout(5 * 1000)
    var zi = ZoneImporter({verbose: true})
    zi.setupDB(function () {
      zi.run(function (stats) {
        expect(stats.NSDocs).to.equal(824)
        expect(stats.BulkChunks).to.equal(9)
        expect(stats.insertedCount).to.equal(824)
        done()
      })
    })

  })

  
})
