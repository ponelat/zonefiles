var es = require('event-stream')
var fs = require('fs')
var DB = require('./db')
var config = require('./config')
var Logger = require('./logger.js')
var deepExtend = require('deep-extend')
var pkg = require('./package.json')
var Glob = require('glob')

// Domain name, with NS record
var NS_REGXEP = /^([\w-]+)\s+(NS|ns)\s+/

module.exports = ZoneImporter

function ZoneImporter(opts) {
  if(!(this instanceof ZoneImporter)) {
    return new ZoneImporter()
  }

  this.opts = deepExtend({
    verbose: false,
  },opts)

  this.log = Logger({
    throttle: config.LOG_THROTTLE,
    prefix: 'Zone Importer'
  })

  // stats('writeConcern', +config.WRITECONCERN, true)

  this.config = require('./config')
  this.zoneFile = config.ZONEFILE

  this.db = DB()

  this.log.h1('Zone-Importer v' + pkg.version)
  this.log.debug(this.zoneFile, 'Zone file')
}

ZoneImporter.prototype.setupDB = function(done) {
  this.db.runSchema(done)
}

function Stats(title) {
  var stats = {title: title}
  return function(record, inc, noStream) {

    if(typeof record !== 'string')
      return stats

    if(inc === true) {
      noStream = true
      inc = 1
    } else if(typeof inc === 'undefined') {
      inc = 1
    } else if(typeof inc !== 'number') {
      inc = 0
    }

    if(noStream) {
      return stats[record] = (stats[record] || 0) + inc
    }

    return es.map(function (line, done) {
      stats[record] = (stats[record] || 0) + inc
      done(null, line)
    })
  }
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

ZoneImporter.waitFor = function waitFor(num) {
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

ZoneImporter.prototype.run = function(file, cb) {
  var that = this
  var stats = Stats(file)
  this.db.connect(function() {
    that.log.h1('Begin importing ' + file)

    fs.createReadStream(file, {flags: 'r'})

    .pipe(es.split())
    .pipe(stats('Lines'))
    .pipe(onlyNSRecords())
    .pipe(stats('Records'))
    .pipe(unique())
    .pipe(stats('UniqueRecords'))
    .pipe(createNSDocument())
    .pipe(stats('NSDocs'))
    .pipe(ZoneImporter.waitFor(config.BULK_COUNT))
    .pipe(stats('BulkChunks'))
    .pipe(that.db.bulkWriter())
    .pipe(es.map(function (bulkRes, done) {
      var count = bulkRes.nInserted + bulkRes.nUpserted
      stats('insertedCount', count || 0, true)

      if(config.WRITECONCERN == 0) {
        // that.log.data('bulk upsert...', stats('BulkChunks'))
      } else {
        that.log.data('Inserted', count)
      }

      done(null, count)
    }))
    .on('end', function() {
      that.log.debug(stats(), 'Stats')
      that.log.h1('Done')
      if(cb) 
        cb(stats())
    })
  })
}

ZoneImporter.prototype.runMulti = function(glob, opts, cb) {
  if(typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  opts = Object.assign({delAfter: false}, opts)
  var that = this
  var stats = {}
  var counter = 0
  that.log.h1('Before Importing ' + glob)
  Glob(glob, function (err, files) {
    that.log.h1('Begin importing ' + files.length + ' files')
    if(files.length <= 0) {
      throw new Error('No files!')
    }
    next()

    function next(err) {
      if(err) 
        throw new Error(err)
      if(counter >= files.length)
        return cb(stats)
      var file = files[counter++]
      that.run(file, function (stat) {
        stats[file] = stat
        that.log.h1('End of file')
        if(opts.delAfter)  
          return fs.unlink(file, next)
        next()
      })
    }
  }) 
}

