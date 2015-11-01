var es = require('event-stream');
var MongoClient = require('mongodb').MongoClient
  , assert = require('assert')
  , urlParser = require('url');

// Connection URL
var opts = {
  url: process.env.MONGO_PORT || 'mongodb://localhost:27017',
  db: process.env.DB || 'test',
  collection: process.env.COLLECTION || 'com'
}
var parts = urlParser.parse(opts.url)
var url = 'mongodb://' + parts.host + '/' + opts.db
var db

function connect(done) {

  // Use connect method to connect to the Server
  MongoClient.connect(url, function(err, _db) {
    db = _db
    assert.equal(null, err);
    console.log("Connected correctly to server");
    console.log('Mongo URL: ' + url)
    console.log('Inserting into: ' + opts.collection)

    if(done){ 
      done()
    }
  });
}

function insert(doc,done) {
  db.collection(opts.collection).insertOne(doc, done)
}

function close() {
  db.close()
}

module.exports.insert = insert
module.exports.close = close
module.exports.connect = connect
