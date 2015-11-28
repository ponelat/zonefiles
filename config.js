var path = require('path')
var deepExtend = require('deep-extend')
var config = {}
reset()

function reset(override){
  override = override || {}
  return deepExtend(config,{
    COLLECTION: 'com',
    ZONEFILE: '',
    ZONEFILES: 'x*',
    DB: 'zones',
    BULK_COUNT: 100,
    LOGEVERYXLINES: '1000',
    WRITECONCERN: '0',
    MONGO_PORT: 'mongodb://localhost:27017',
    _DBPATH: path.join(__dirname, '_db/'),
    reset: reset
  }, process.env, override)
}

module.exports = config
