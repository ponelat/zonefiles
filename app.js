var zi = require('./zone-importer')()
zi.setupDB(function() {
  zi.run()
})
