var es = require('event-stream');
var MongoClient = require('mongodb').MongoClient
  , assert = require('assert')
  , urlParser = require('url');

var bulkMongo = require('bulk-mongo')
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
  this.db
}

DB.prototype.connect = function(done) {
  var that = this

  // Use connect method to connect to the Server
  MongoClient.connect(this.url, function(err, _db) {
    assert.equal(null, err);

    that.db = _db

    // factory_function = bulkMongo(db);

    that.coll = that.db.collection(config.COLLECTION)

    that.log.log("Connected correctly to server");
    that.log.data('Mongo URL', that.url)
    that.log.data('Inserting into',config.COLLECTION)

    if(done){ 
      done()
    }
  });
}

DB.prototype.insert = function (doc,done) {
  return this.coll.insertOne(doc, done)
}

DB.prototype.insertMany = function (docs,done) {
  return this.coll.insertMany(docs, {w:0}, done)
}

DB.prototype.bulkWriter = function () {
  var that = this

  return es.map(function (domains, done) {
    that.insertMany(domains)
    .then(function (res) {
      done(null, res)
    })
    .catch(function (err) {
      done(err)
    })
  })

}
