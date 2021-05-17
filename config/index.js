const envLoader = require('@b2wads/env-o-loader')

module.exports = {
  watcherConfig: Object.freeze(envLoader('./watcher.yaml')),
}
