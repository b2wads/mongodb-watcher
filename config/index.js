const envLoader = require('@b2wads/env-o-loader')

module.exports = {
  watcherConfig: envLoader('./watcher.yaml'),
}
