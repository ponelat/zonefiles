var es = require('event-stream')
var fs = require('fs')
var DB = require('./db')

// Domain name, with NS record
var reg = /^([\w-]+)\s+(NS|ns)\s+/
var rejectedLines = 0
var acceptedLines = 0
var totalLines = 0

var zones = {}

DB.connect(function() {

  fs.createReadStream('com.zone', {flags: 'r'})
  .pipe(es.split())
  .pipe(es.map(function(line, done){

    totalLines++

    var matches = reg.exec(line)

    if(!matches) {
      rejectedLines++
      // console.log('line "' + line + '"');
      process.stdout.write('x')
      return done(null, line)
    }

    acceptedLines++

    var record = {
      domain: matches[1]
    }

    DB.insert('com', record, function(err, res) {
      process.stdout.write('.')
      done(null, line)
    })

  }))
  .on('end', function() {
    DB.close()
    console.log('Done');
    console.log('rejectedLines', rejectedLines);
    console.log('acceptedLines', acceptedLines);
    console.log('totalLines', totalLines);
  })
})

