config = require("cson-config").load()
kafka = require "kafka-node"
kafkaClient = new kafka.Client config.kafka.zookeeper
kafkaConsumer = new kafka.Consumer kafkaClient, [topic: "aggregator"]
kafkaProducer= new kafka.Producer kafkaClient

{Iconv} = require "iconv"
iconv = new Iconv "UTF-8", "ASCII//IGNORE"

stopwords = {}
stopwords[i] = yes for i in require "./stopwords.json"

counts = {}
total = 0
keywords = []

setInterval ->
	for topic, num of total
		o =
			total: num
		kafkaProducer.send [topic: topic, messages: [JSON.stringify o]], (e) ->
			console.log "kafka:", e if e
, 200

setInterval ->
	sorted = Object.keys counts
	sorted.sort (a, b) -> counts[b] - counts[a]
	return unless sorted.length
	o =
		keywords: keywords[topic]
		counts: (key: i, value: counts[i] for i in sorted[0..50])
	kafkaProducer.send [
		topic: topic
		messages: [JSON.stringify o]
	], (e) ->
		console.log "kafka:", e if e
		delete counts[i] for i in sorted[250..] if sorted.length > 500
, 5000

processMessage = (message) ->
	try
		decoded = JSON.parse message.value
		words = decoded
			.tweet
			.text
			.split " "
			.map (v) -> iconv.convert(v).toString()
			.filter (v) -> v.length > 2 and v not of stopwords and not v.match /(http|[&\/])/
		total[decoded.topic]++
		keywords ?= decoded.keywords
		for word in words
			word = word.toLowerCase().replace(/[,;:!.})(=-]/ig, "").trim()
			if word.length > 2 and word not in decoded.keywords
				counts[word] ?= 0
				counts[word]++
	catch e
		console.error e

kafkaConsumer.on "e", console.error
kafkaConsumer.on "message", processMessage
