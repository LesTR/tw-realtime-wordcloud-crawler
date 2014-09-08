require('cson-config').load()
config = process.config
colors = require 'colors'
kafka = require 'kafka-node'
Twitter = require 'node-tweet-stream'
moment = require 'moment'
debug = require('debug')('crawler')
kafkaClient = new kafka.Client config.kafka.zookeeper
kafkaProducer = new kafka.Producer kafkaClient

kafkaProducer.on 'error', (err)->
	console.log "Kafka ERROR pYco: ".red, err

kafkaProducer.on 'ready', ()->
	debug "Kafka producer ready"
	for t in config.twitter.tokens
		c =
			consumer_key: config.twitter.consumer_key
			consumer_secret: config.twitter.consumer_secret
			token: t.token
			token_secret: t.token_secret

		console.log c
		s = new Twitter c
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
			publishTweet config.keywords, t, ()->

		for keyword in config.keywords
			debug "tracking keyword: #{keyword}"
			s.track keyword



publishTweet = (keywords, tweet)->
	debug "publish tweet with id: #{tweet.id}"
	m =
		keywords: keywords
		tweet: tweet
	message = JSON.stringify m
	kafkaProducer.send [
		{topic: "aggregator", messages:[message], partition: 0}
	],(err, data)->
		console.log arguments


#model = require './lib/model'

#model.start config
