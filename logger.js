var chalk = require('chalk')
var prettyjson = require('prettyjson')
var deepExtend = require('deep-extend')
var Meta = function(str){ this.str = str; return this}

module.exports = Logger

function Logger(opts) {

  if(!(this instanceof Logger)) {
    return new Logger(opts)
  }

  if(typeof opts === 'string') {
    opts = {prefix: opts}
  }

  deepExtend(this,{
    throttle: 1,
    prefix: 'LOG',
    colors: true,
    ppOpts: {
      colors: opts.colors || true
    }
  },opts)

  if(typeof colors === 'undefined') {
    colors = true
  }
  this.count = 0

  this.log('Logger Throttled at', chalk.bold(this.throttle) )
}


Logger.prototype.data = function (item, data) {
  this.log(chalk.yellow.underline(item) +':', chalk.bold(data))
}

Logger.prototype.debug = function (data, title) {
  if(title) {
    this.log(title)
  }
  this.print(prettyjson.render(data, this.ppOpts))
}

Logger.prototype.throttled = function (str) {
  if((this.count++ % this.throttle) === 0) {
    this.count = 1
    this.log(str)
  }
}

function chars(num,chars) {
  chars = chars || '-'
  return (new Array(num)).join(chars)
}

Logger.prototype.prefixLength = function () {
  return this.prefix.length + 2
}

Logger.prototype.h1 = function (str) {
  this.log(chalk.green(chars(10,'=')), chalk.bold(str), chalk.green(chars(10,'=')))
}

Logger.prototype.title = function (title) {
  var line = chars(5, '=')
  this.log(title)
  this.skipPrefix(chalk.grey(line))
}

Logger.prototype.prettyPrefix = function () {
  return '['+this.prefix+']'
}

Logger.prototype.log = function () {
  var args = asArray(arguments)
  this._log({
    prefix: this.prettyPrefix(),
    args: args
  })
}

Logger.prototype.print = function (all) {
  if(Array.isArray(all))
    console.log.apply(console, all)
  else
    console.log(all)
}

Logger.prototype.skipPrefix = function (str) {
  var args = asArray(arguments)
  this._log({
    prefix: chars(this.prettyPrefix().length + 1, ' '),
    args: args
  })
}

function asArray(args, num) {
  num = num || 0
  return Array.prototype.slice.call(args, num)
}

Logger.prototype._log = function (opts) {
  opts = opts || {
    prefix: this.prefix,
    args: []
  }
  var prefix = chalk.grey(opts.prefix)
  var all = [prefix].concat(opts.args)
  this.print(all)
}

