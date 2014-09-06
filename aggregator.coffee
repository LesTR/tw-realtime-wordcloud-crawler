config = require("cson-config").load()
kafka = require "kafka-node"
kafkaClient = new kafka.Client config.kafka.zookeeper
kafkaConsumer = new kafka.Consumer kafkaClient, [topic: "mrdka", partition: 0]
kafkaProducer= new kafka.Producer kafkaClient, [topic: "mrdka", partition: 0]

stopwords = {}
stopwords[i] = yes for i in require "./stopwords.json"

counts = {}

setInterval ->
	sorted = Object.keys counts
	sorted.sort (a, b) -> counts[b] - counts[a]
	return unless sorted.length
	o = (key: i, value: counts[i] for i in sorted[0..50])
	kafkaProducer.send [topic: "jebka", messages: [JSON.stringify o]], (e) ->
		console.error e if e
		delete counts[i] for i in sorted[250..] if sorted.length > 500
, 2000

processMessage = (message) ->
	try
		tweet = JSON.parse(message.value).tweet.text
		words = tweet.split(" ").filter (v) -> v.length > 2 and v not of stopwords
		for word in words
			counts[word] ?= 0
			counts[word]++
	catch e
		console.error e

kafkaConsumer.on "e", console.error
kafkaConsumer.on "message", processMessage
