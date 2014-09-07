debug = require('debug')('model')
ZK = require 'zkjs'
async = require 'async'
kafka = require 'kafka-node'
colors = require 'colors'
Twitter = require 'node-tweet-stream'

initialized = no
zk = null
kafkaClient = null
kafkaProducer = null
actualKeywords = {}
config = null

allStreams = {}

module.exports.start = (params,cb)->
	debug "keywords init"
	config = params
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


refreshKeywords = (cb)->
	debug "refreshing keywords"
	x = {}
	zk.getChildren "/keywords",updateKeywords, (err, streams, zstat)->
		async.eachLimit streams, 10, (stream, next)->
			debug "getting data for stream #{stream}"
			zk.get "/keywords/#{stream}", null, (err, value, zstat)->
				return next err if err
				try
					d = JSON.parse value.toString()
				catch e
					return next e
				x[d.topic] = d
				next()
		,(err)->
			return cb err if err
			running = []
			for topic,s of x
				#if allStreams[s.topic]
				#	running.push s.topic
				#else
					#trackKeyword s
				trackKeyword s

			return cb(null,x)


updateKeywords = (info)->
	console.log arguments
	if info.path is "/keywords" and info.type is "child"
		refreshKeywords (err, k)->
			debug "keywords refreshed"



trackKeyword = (keywordStructure)->
	debug "track topic #{keywordStructure.topic}"
	c =
		consumer_key: config.twitter.consumer_key
		consumer_secret: config.twitter.consumer_secret
		token: keywordStructure.token
		token_secret: keywordStructure.secret

	unless allStreams[keywordStructure.topic]
		s = new Twitter c
		s.keywords = keywordStructure.keywords
		s.topic = keywordStructure.topic
		s.lastUpdate = new Date().getTime()

		s.on 'error', (error)->
			console.log "TW error pYco".red, error

		s.on 'tweet', (tweet)->
			t =
				id: tweet.id_str
				text: tweet.text
				user:
					id: tweet.user.id_str
					description: tweet.user.description
					screenname: tweet.user.screen_name
				lang: tweet.lang
				entities: tweet.entities
				timestamp: tweet.timestamp_ms
				source: tweet.source
				topicLastUpdate: s.lastUpdate

			publishTweet keywordStructure, t, ()->

		allStreams[keywordStructure.topic] = s
	else
		s = allStreams[keywordStructure.topic]

	for k in s.keywords
		s.untrack k
	for k in keywordStructure.keywords
		s.track k

untrackKeyword = (stream)->
	debug "untrackKeyword #{stream.topic}"
	s = allStreams[stream.topic]
	for k in stream.keywords
		s.untrack k
	delete allStreams[stream.topic]



publishTweet = (keywordStructure, tweet, cb)->

	debug "publishTweet for topic #{keywordStructure.topic} with id: #{tweet.id}"
	m =
		id: 8
		topic: keywordStructure.topic
		keywords: keywordStructure.keywords
		tweet: tweet
	message = JSON.stringify m
	kafkaProducer.send [
		{topic: "aggregator", messages:[message], partition: 0}
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
