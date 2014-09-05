Consumer = require('prozess').Consumer

options =
	host : "192.168.218.22"
	topic : 'test'
	partition : 0
	offset : 0


consumer = new Consumer options


consumer.connect (err) ->
	console.log "Connect error", err if err


	setInterval () ->
		console.log "==================================================================="
		console.log new Date()
		console.log "consuming: " + consumer.topic
		consumer.consume (err, message) ->
			console.log err if err

			console.log message
	, 2000