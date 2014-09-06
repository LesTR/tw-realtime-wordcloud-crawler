debug = require('debug')('model')
ZK = require 'zkjs'
async = require 'async'
kafka = require 'kafka-node'
colors = require 'colors'

initialized = no
zk = null
kafkaClient = null
kafkaProducer = null
tw = null
actualKeywords = {}
module.exports.init = (params,t,cb)->
	debug "keywords init"
	tw = t
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
				console.log "Kafka ERROR pYco: ".red, err

			kafkaProducer.on 'ready', ()->
				debug "Kafka producer ready"
				next()
	],(err)->
		debug "Initialization completed"
		cb err if cb

module.exports.myKeywords = (cb)->
	debug "call myKewords"
	refreshKeywords	cb

refreshKeywords = (cb)->
	debug "refreshing keywords"
	zk.getChildren "/keywords",updateKeywords, (err, keywords, zstat)->
		x = []
		y = {}
		for k in keywords
			x.push k
			if actualKeywords[k]
				delete actualKeywords[k]
			else
				trackKeyword k
				y[k] = yes

		for k of actualKeywords
			untrackKeyword k
		actualKeywords = y

		return cb(null,x)


updateKeywords = (info)->
	if info.path is "/keywords" and info.type is "child"
		refreshKeywords (err, k)->
			debug "keywords refreshed",k



trackKeyword = (keyword)->
	debug "trackKeyword #{keyword}"
	tw.track keyword

module.exports.trackKeyword = trackKeyword
untrackKeyword = (keyword)->
	debug "untrackKeyword #{keyword}"
	tw.untrack keyword
module.exports.untrackKeyword = untrackKeyword
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
			refreshKeywords cb
