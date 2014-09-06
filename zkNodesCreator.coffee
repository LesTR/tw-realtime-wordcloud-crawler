require('cson-config').load()
config = process.config
ZK = require 'zkjs'

zk = new ZK config.zookeeper

zk.start (err)->
	console.log err if err

zk.on 'expired', ()->
	console.log "ZK session expired ... reconneting".red
	zk.start()



module.exports.createKeywordPath = (keyword, next) ->

	path = "keywords/" + keyword
	zk.mkdirp path, (err) ->
		return next err if err
		console.log "Path created"


# usage
#module.exports.createKeywordPath "foobar12"


