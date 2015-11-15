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

var db = DB()

function dbClean(done) {
  db.connect(function () {
    db.db.dropCollection('com', function (err) {
      if(err && (err.message).match(/ns not found/)) {
        return done(); // Ignore 'ns not found' error if collection did not exist
      }
      done(err)
    });
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
  db.db.close(function () {
    mongo.kill()
    mongo.once('exit', function () {
      logger.log('Mongo killed --------------')
      done()
    })
  })
}

describe('Zonefile importer', function(){

  before(function(done){
    logger.log('Opening DB -------------')
    logger.log('..on: ', config._DBPATH)
    logger.log('..collection: ', config.COLLECTION)
    dbSetup(done)
  })

  after(function(done){
    this.timeout(5 * 1000)
    logger.log('Killing server ---------------')
    dbKill(done)
  })

  beforeEach(function(done){
    config.reset({
      LOGEVERYXLINES: 1,
      ZONEFILE: path.resolve(__dirname, '../test-zones/com.zone'),
      DB: 'test'
    })
    dbClean(done)
  })

  it('there should not be any records', function(done){

    var res = db.db.collection(config.COLLECTION).find({}).count()
    // require('../app.js')
    res.then(function (count) {
      expect(count).to.equal(0)
      done()
    })
    
  })

  it('should insert NS records', function(done){

    this.timeout(5 * 1000)
    var zi = ZoneImporter({verbose: true})
    zi.run(function (count) {
      expect(count.insertedCount).to.equal(824)
      done()
    })

  })
  
})
