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
	data = JSON.parse fs.readFileSync "./aggregator.json"
	counts = data.counts
	total = data.total
catch
	counts = {}
	total = 0

keywords = null

setInterval ->
	o = {keywords, total}
	kafkaProducer.send [topic: "keynote", messages: [JSON.stringify o]], (e) ->
		console.log "kafka:", e if e
, 200

setInterval ->
	sorted = Object.keys counts
	sorted.sort (a, b) -> counts[b] - counts[a]
	return unless sorted.length
	o =
		counts: (key: i, value: counts[i] for i in sorted[0..50])
	kafkaProducer.send [
		topic: "keynote"
		messages: [JSON.stringify o]
	], (e) ->
		console.log "kafka:", e if e
		if sorted.length > 1000
			for word, index in sorted
				if index < 250
					counts[word] = (250 - index) * 10
				else
					delete counts[word]
		fs.writeFile "./aggregator.json", JSON.stringify({counts, total}), (e) ->
			console.log e if e
, 5000

processMessage = (message) ->
	try
		decoded = JSON.parse message.value
		keywords ?= decoded.keywords
		words = decoded
			.tweet
			.text
			.toLowerCase()
			.replace /[,;:!.})(=-?"]/ig, " "
			.split " "
			.map (v) -> iconv.convert(v.trim()).toString()
			.filter (v) ->
				v.length > 2 and
				v not of stopwords and
				not v.match /(http|[&\/])/ and
				not v.match /^\d+$/
				v not in keywords
		total++
		for word in words
			counts[word] ?= 0
			counts[word]++
	catch e
		console.error e

kafkaConsumer.on "error", console.error
kafkaProducer.on "error", console.error
kafkaConsumer.on "message", processMessage
