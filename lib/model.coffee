debug = require('debug')('keywords')
ZK = require 'zkjs'
async = require 'async'
kafka = require 'kafka-node'
colors = require 'colors'

initialized = no
zk = null
kafkaClient = null
kafkaProducer = null

module.exports.init = (params,cb)->
	debug "keywords init"
	zk = new ZK params.zookeeper
	async.parallel [
		(next)->
			debug "Initializing zookeeper"
			zk.on 'expired', ()->
				console.log "ZK session expired ... reconneting".red
				zk.start()
			zk.start (err)->
				return next err if err
				createZKstructures params, next
		(next)->
			debug "Initializing kafka #{params.kafka.zookeeper}"

			kafkaClient = new kafka.Client params.kafka.zookeeper
			kafkaProducer = new kafka.Producer kafkaClient

			kafkaProducer.on 'error', (err)->
				console.log arguments

			kafkaProducer.on 'ready', ()->
				debug "Kafka producer ready"
				next()
	],(err)->
		debug "Initialization completed"
		cb err

module.exports.myKewords = ()->
	debug "call myKewords"
	a = ()->
		console.log arguments
	zk.getChildren "/keywords",a, (err, keywords, zstat)->
		console.log arguments
	return ["bieber","whereismike"]
module.exports.trackKeyword = (keyword)->
	debug "trackKeyword", arguments
module.exports.untrackKeyword = (keyword)->
	debug "untrackKeyword", arguments

module.exports.publishTweet = (tweet, cb)->

	debug "publishTweet with id: #{tweet.id}"
	m =
		id: 8
		tweet: tweet
	message = JSON.stringify m
	kafkaProducer.send [
		{topic: "mrdka", messages:[message], partition: 0}
	],(err, data)->
		cb(err)

createZKstructures = (params,cb)->

	paths = ["/keywords", "/crawlers"]

	async.eachSeries paths, (path, next)->
		zk.exists path, (err, exists)->
			debug "Checking path #{path}:",exists
			return next err if err
			return next null if exists
			zk.mkdirp path, (err)->
				debug "Creating path #{path}", err
				initialized = no if err
				return next err
	,(err)->
		return cb err if err
		#create my ephemeral node
		zk.create "/crawlers/node", "", ZK.create.EPHEMERAL_SEQUENCE, (err,path)->
			cb err
