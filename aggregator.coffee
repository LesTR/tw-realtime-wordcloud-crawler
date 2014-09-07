config = require("cson-config").load()
kafka = require "kafka-node"
kafkaClient = new kafka.Client config.kafka.zookeeper
kafkaConsumer = new kafka.Consumer kafkaClient, [topic: "aggregator"]
kafkaProducer= new kafka.Producer kafkaClient

stopwords = {}
stopwords[i] = yes for i in require "./stopwords.json"

counts = {}
total = {}
keywords = {}

setInterval ->
	for topic, items of counts
		do (topic, items) ->
			sorted = Object.keys items
			sorted.sort (a, b) -> items[b] - items[a]
			return unless sorted.length
			o =
				total: total[topic]
				keywords: keywords[topic]
				counts: (key: i, value: items[i] for i in sorted[0..50])
			kafkaProducer.send [topic: topic, messages: [JSON.stringify o]], (e) ->
				console.error e if e
				delete items[i] for i in sorted[250..] if sorted.length > 500
, 5000

processMessage = (message) ->
	try
		decoded = JSON.parse message.value
		words = decoded.tweet.text.split(" ").filter (v) ->
			v.length > 2 and v not of stopwords
		counts[decoded.topic] ?= {}
		total[decoded.topic] ?= 0
		total[decoded.topic]++
		keywords[decoded.topic] = decoded.keywords
		for word in words
			word = word.toLowerCase().replace(/[,;:!.})(=-]/ig, "").trim()
			if word.length > 2 and word not in decoded.keywords
				counts[decoded.topic][word] ?= 0
				counts[decoded.topic][word]++
	catch e
		console.error e

kafkaConsumer.on "e", console.error
kafkaConsumer.on "message", processMessage
