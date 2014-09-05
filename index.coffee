require('cson-config').load()
config = process.config

Twitter = require 'node-tweet-stream'

tw = new Twitter config.twitter

publishTweet = (tweet, keyword)->
	console.log keyword, JSON.stringify tweet

	tw.on 'tweet', (tweet)=>
	#console.log tweet
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
	console.log t

	for keyword in keywords
		console.log keyword
	keywords.each (keyword)=>
		console.log keyword
		if t.text.toLowerCase().indexOf(keyword.toLowerCase()) isnt -1
			publishTweet t, keyword

tw.on 'error', (error)->
	console.log error



keywords = ["rusia"]

for keyword in keywords

	tw.track keyword
