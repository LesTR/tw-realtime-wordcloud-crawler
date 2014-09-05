Producer = require('prozess').Producer


config =
	host: "192.168.218.22"
producer = new Producer 'test', config

producer.connect()


console.log "producing for", producer.topic

producer.on "error", (err) ->
	console.log "Producer error:" + err


setInterval () ->
	producer.send "jebka", (err) ->
		console.log "Err in producing", err if err

, 1000











