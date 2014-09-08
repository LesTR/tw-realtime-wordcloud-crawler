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
		fs.writeFile "./aggregator.json", JSON.stringify({counts, total}), (e) ->
			console.log e if e
		if sorted.length > 1000
			delete counts[i] for i in sorted[250..]
			counts[i] = 250 - i for i in sorted[0..250]
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
		total++
		keywords ?= decoded.keywords
		for word in words
			word = word.toLowerCase().replace(/[,;:!.})(=-?]/ig, "").trim()
			if word.length > 2 and word not in decoded.keywords
				counts[word] ?= 0
				counts[word]++
	catch e
		console.error e

kafkaConsumer.on "error", console.error
kafkaProducer.on "error", console.error
kafkaConsumer.on "message", processMessage
