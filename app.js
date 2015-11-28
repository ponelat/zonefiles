var zi = require('./zone-importer')()
var config = require('./config')
zi.setupDB(function() {
  if(config.ZONEFILES) {
    zi.runMulti(config.ZONEFILES, {delAfter: true}, zi.db.close.bind(zi))
    return
  } else if(config.ZONEFILE) {
    zi.run(config.ZONEFILE, zi.db.close.bind(zi))
    return
  } else {
    console.log('Not Running, ZONEFILES? empty')
    console.log('config', config)
  }
})
