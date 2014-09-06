kafka = require('kafka-node')

client = new kafka.Client "192.168.218.53:2181/kafka0.8"

producer = new kafka.Producer client
consumer = new kafka.Consumer client, [{topic: 'first', partition: 0}], autoCommit: false
offset = new kafka.Offset client

consumer.on "message", (msg) ->
	console.log "MSG:", msg

consumer.on "error", (err) ->
	console.log "ERR:", err
