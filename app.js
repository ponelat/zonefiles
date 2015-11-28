var zi = require('./zone-importer')()
zi.setupDB(function() {
  zi.runMulti('x*', {delAfter: true}, zi.db.close.bind(zi))
})
