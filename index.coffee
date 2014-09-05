require('cson-config').load()
config = process.config

Twitter = require 'node-tweet-stream'

tw = new Twitter config.twitter

tw.on 'tweet', (tweet)->
	console.log tweet
tw.on 'error', (error)->
	console.log error


tw.track "pizza"
