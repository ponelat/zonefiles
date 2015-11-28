var expect = require('chai').expect
var DB = require('../db')
var spawn = require('child_process').spawn
var Glob = require('glob')
var path = require('path')
var fs = require('fs')
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
      ZONEFILE: path.resolve(__dirname, '../com.zone'),
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


  describe('Single Run', function(){

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
        zi.run('com.zone', function (stats) {
          expect(stats.NSDocs).to.equal(824)
          expect(stats.BulkChunks).to.equal(9)
          expect(stats.insertedCount).to.equal(824)
          done()
        })
      })

    })

    it('should not die, if duplicates exist', function(done){

      this.timeout(5 * 1000)
      var zi = ZoneImporter()
      zi.setupDB(function () {
        zi.db.insert({domain: 'LADIESFORMALWEAR'}, function () {

          zi.run('com.zone', function (stats) {
            expect(stats.NSDocs).to.equal(824)
            expect(stats.BulkChunks).to.equal(9)
            expect(stats.insertedCount).to.equal(823)
            done()
          })

        })
      })

    })


  })

  describe('Multi Run', function(){

    // beforeEach(function(done){
    //   splitFileInto('com.zone', 10, done)
    // })

    // afterEach(function(done){
    //   Glob('com.zone_*', function (err, files) {
    //     var counter = 0
    //     next()
    //     function next() {
    //       if(counter >= files.length)
    //         return done()
    //       var file = files[counter++]
    //       console.log('file', file)
    //       fs.unlink(file, next)
    //     }
    //   })
    // })

    it('should parse/upload each file', function(done){

      this.timeout(5 * 1000)

      var zi = ZoneImporter({verbose: true})
      zi.runMulti('com.zone_*', function (stats) {
        expect(Object.keys(stats)).to.have.length(10)
        done()
      })
    })

    it('should parse/upload each file and del file afterwards', function(done){

      this.timeout(5 * 1000)

      var zi = ZoneImporter()
      zi.runMulti('com.zone_*', function (stats) {
        expect(Object.keys(stats)).to.have.length(10)
        Glob('com.zone_*', function (err, files) {
          console.log('files', files)
          expect(files).to.have.length(0)
          done()
        })
      })
    })
  })

})

function splitFileInto(file, parts, done) {
  var totalLines = 0
  var anotherTotalLines = 0

  fs.readFileSync(file)
    fs.createReadStream(file, {flags: 'r'})
    .pipe(es.split())
    .pipe(es.map(count))
    .on('end', function () {
      var linesPerPart = (totalLines-1) / parts

      fs.createReadStream(file, {flags: 'r'})
      .pipe(es.split())
      .pipe(ZoneImporter.waitFor(linesPerPart))
      .pipe(es.map(function (lines, next) {
        anotherTotalLines += lines.length
        var filename = file + '_' + numToAlpha(Math.floor(anotherTotalLines / linesPerPart) - 1 )
        if(anotherTotalLines > totalLines) {
          return done(null, lines)
        }
        fs.writeFile(filename, lines.join('\n'), function () {
          next(null, lines)
        })
      }))
      .on('end', done)
    })

  function count(line, next) {
    totalLines++
    next(null, line)
  }
}

const ALPHA  = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
function numToAlpha(num) {
  return ALPHA[num]
}
