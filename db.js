var es = require('event-stream');
var MongoClient = require('mongodb').MongoClient
  , assert = require('assert');

// Connection URL
var url = 'mongodb://localhost:27017/test';
var db

function connect(done) {

  // Use connect method to connect to the Server
  MongoClient.connect(url, function(err, _db) {
    db = _db
    assert.equal(null, err);
    console.log("Connected correctly to server");

    if(done){ 
      done()
    }
  });
}

function insert(coll, doc,done) {
  db.collection(coll).insertOne(doc, done)
}

function close() {
  db.close()
}

module.exports.insert = insert
module.exports.close = close
module.exports.connect = connect
