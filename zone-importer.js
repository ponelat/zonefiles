var es = require('event-stream')
var fs = require('fs')
var DB = require('./db')
var config = require('./config')
var Logger = require('./logger.js')
var deepExtend = require('deep-extend')

// Domain name, with NS record
var NS_REGXEP = /^([\w-]+)\s+(NS|ns)\s+/

module.exports = ZoneImporter

function ZoneImporter(opts) {
  if(!(this instanceof ZoneImporter)) {
    return new ZoneImporter()
  }

  opts = deepExtend({
    verbose: false
  },opts)

  this.log = Logger({
    throttle: config.LOG_THROTTLE,
    prefix: 'Zone Importer'
  })

  this._stats = {}

  this.config = require('./config')
  this.zoneFile = config.ZONEFILE

  this.db = DB()
  this.log.data('Zone file', this.zoneFile)

}

ZoneImporter.prototype.stats = function(record, inc, noStream) {

  if(typeof record !== 'string')
    return this._stats

  if(inc === true) {
    noStream = true
    inc = 1
  } else if(typeof inc === 'undefined') {
    inc = 1
  } else if(typeof inc !== 'number') {
    inc = 0
  }

  if(noStream) {
    return this._stats[record] = (this._stats[record] || 0) + inc
  }

  var that = this
  return es.map(function (line, done) {
    that._stats[record] = (that._stats[record] || 0) + inc
    done(null, line)
  })
}

function unique() {
  var lastLine = ''
  return es.map(function (line, done) {
    if(lastLine == line) 
      return done()
    lastLine = line
    done(null, line)
  })
}

function onlyNSRecords() {
  return es.map(function (line, done) {
    var matches = NS_REGXEP.exec(line)
    if(!matches) {
      return done()
    }
    done(null, matches[1])
  })
}

function createNSDocument() {
  return es.map(function (line, done) {
    done(null, {
      domain: line
    })
  })
}

function waitFor(num, that) {
  var arr = []
  return es.through(function write(chunk, eh) {
    arr.push(chunk)

    if(arr.length >= num) {
      this.emit('data', arr)
      arr = []
      return
    } 

  }, function end(eh) {
    if(arr.length > 0) {
      this.emit('data', arr)
    }
    this.emit('end')
  })
}

ZoneImporter.prototype.run = function(cb) {
  var that = this
  that.log.h1('Callback')
  this.log.debug(cb)

  this.db.connect(function() {
    that.log.h1('Begin importing')

    fs.createReadStream(that.zoneFile, {flags: 'r'})

    .pipe(es.split())
    .pipe(that.stats('Lines'))
    .pipe(onlyNSRecords())
    .pipe(that.stats('Records'))
    .pipe(unique())
    .pipe(that.stats('UniqueRecords'))
    .pipe(createNSDocument())
    .pipe(that.stats('NSDocs'))
    .pipe(waitFor(config.BULK_COUNT, that))
    .pipe(that.stats('BulkChunks'))
    .pipe(that.db.bulkWriter())
    .pipe(es.map(function (count, done) {
      that.stats('insertedCount', count.insertedCount || 0, true)
      done(null, count)
    }))
    .on('end', function() {
      that.db.db.close(function () {

        that.log.debug(that.stats(), 'Stats')
        that.log.h1('Done')
        that.log.debug(cb, 'Callback')
        cb(that.stats())
        
      })
    })
  })
}

