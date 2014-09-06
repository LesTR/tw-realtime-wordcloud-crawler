require('cson-config').load()
config = process.config
colors = require 'colors'
model = require './lib/model'


Twitter = require 'node-tweet-stream'

tw = new Twitter config.twitter

publishTweet = (tweet, keyword)->
	#console.log keyword.green, JSON.stringify tweet

tw.on 'tweet', (tweet)=>
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

	model.publishTweet t, ()->

tw.on 'error', (error)->
	console.log "ERROR pYco!".red, error




model.init config, ()=>
	for keyword in model.myKewords()
		tw.track keyword
