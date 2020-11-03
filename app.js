const { Watcher } = require('./src')

const { watcherConfig } = require('./config')

const watcher = new Watcher(watcherConfig)

process.on("SIGINT", async () => {
  console.error("Received interruption signal, stopping watcher")
  await watcher.stop()
})

console.log("Starting watcher with configs:", watcherConfig)
watcher.start()

