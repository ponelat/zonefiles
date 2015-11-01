var es = require('event-stream')
var fs = require('fs')
var DB = require('./db')

// Domain name, with NS record
var reg = /^([\w-]+)\s+(NS|ns)\s+/
var rejectedLines = 0
var acceptedLines = 0
var totalLines = 0

var zoneFile = process.env.ZONEFILE || 'com.zone'

var zones = {}

var loggerCount = 0
var logOnlyOn = process.env.LOGEVERYXLINES || 1000 // log every 100th time, for performance

function logLine(str) {
  if(loggerCount++ % logOnlyOn === 0) {
    loggerCount = 1
    process.stdout.write(str)
  }
}

console.log('Logging every ' + logOnlyOn + ' lines')

DB.connect(function() {

  fs.createReadStream(zoneFile, {flags: 'r'})
  .pipe(es.split())
  .pipe(es.map(function(line, done){

    totalLines++

    var matches = reg.exec(line)

    if(!matches) {
      rejectedLines++
      logLine('x')
      return done(null, line)
    }

    acceptedLines++

    var record = {
      domain: matches[1]
    }

    DB.insert(record, function(err, res) {
      logLine('.')
      done(null, line)
    })

  }))
  .on('end', function() {
    DB.close()
    console.log('\nDone');
    console.log('rejectedLines', rejectedLines);
    console.log('acceptedLines', acceptedLines);
    console.log('totalLines', totalLines);
  })
})

