config = require("cson-config").load()
kafka = require "kafka-node"
kafkaClient = new kafka.Client config.kafka.zookeeper
kafkaConsumer = new kafka.Consumer kafkaClient, [topic: "aggregator"]
kafkaProducer = new kafka.Producer kafkaClient
fs = require "fs"

{Iconv} = require "iconv"
iconv = new Iconv "UTF-8", "ASCII//IGNORE"

stopwords = {}
stopwords[i] = yes for i in require "./stopwords.json"

try
	counts = JSON.parse fs.readFile "./data.json"
	total = Object.keys(counts).length
	console.log counts, total
catch
	counts = {}
	total = 0

keywords = []

setInterval ->
	o = {total}
	kafkaProducer.send [topic: "keynote", messages: [JSON.stringify o]], (e) ->
		console.log "kafka:", e if e
, 200

setInterval ->
	sorted = Object.keys counts
	sorted.sort (a, b) -> counts[b] - counts[a]
	return unless sorted.length
	o =
		keywords: keywords
		counts: (key: i, value: counts[i] for i in sorted[0..50])
	kafkaProducer.send [
		topic: "keynote"
		messages: [JSON.stringify o]
	], (e) ->
		console.log "kafka:", e if e
		fs.write "./aggregator.json", JSON.stringify counts
		delete counts[i] for i in sorted[250..] if sorted.length > 500
, 5000

processMessage = (message) ->
	console.log "."
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

kafkaConsumer.on "error", console.error
kafkaProducer.on "error", console.error
kafkaConsumer.on "message", processMessage
