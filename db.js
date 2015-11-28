var es = require('event-stream');
var MongoClient = require('mongodb').MongoClient
  , assert = require('assert')
  , urlParser = require('url');

var Logger = require('./logger')

// Stream 
var es = require('event-stream')

// Connection URL
var config = require('./config')

module.exports = DB

function DB() {
  if(!(this instanceof DB)) {
    return new DB()
  }

  var parts = urlParser.parse(config.MONGO_PORT)
  this.url = 'mongodb://' + parts.host + '/' + config.DB

  this.log = Logger('DB')
}

DB.prototype.close = function(done) {
  this.db.close(function () {
    this.db = void 0
    if(typeof done === 'function')
      done()
  })
}

DB.prototype.connect = function(done) {
  var that = this
  if(this.db) {
    if(done) 
      done()
    return
  }

  // Use connect method to connect to the Server
  MongoClient.connect(this.url, function(err, _db) {
    assert.equal(null, err);

    that.db = _db


    that.log.log("Connected correctly to server");
    that.log.data('Mongo URL', that.url)
    that.log.data('Inserting into',config.COLLECTION)

    if(done){ 
      done()
    }
  });
}

DB.prototype.runSchema = function(done) {
  var that = this
  that.connect(function () {
    that.db.createCollection(config.COLLECTION, function (coll) {
      that.coll().ensureIndex({domain: 1}, {unique: true}, done)
    })
  })
}

DB.prototype.coll = function(done) {
  return this.db.collection(config.COLLECTION)
}

DB.prototype.insert = function (doc,done) {
  return this.coll().insertOne(doc, done)
}

DB.prototype.insertMany = function (docs,done) {
  return this.coll().insertMany(docs, this.writeConcern(), done)
}


DB.prototype.writeConcern = function () {
  return {w: +config.WRITECONCERN}
}

DB.prototype.bulkWriter = function () {
  var that = this

  return es.map(function (domains, done) {
    // that.insertMany(domains, done)
    var batch = that.coll().initializeUnorderedBulkOp(that.writeConcern())
    domains.forEach(function (domain) {
      batch.find(domain).upsert().updateOne(domain)
    })
    batch.execute(done)
  })

}
