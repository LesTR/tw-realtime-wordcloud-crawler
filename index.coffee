require('cson-config').load()
config = process.config
colors = require 'colors'
model = require './lib/model'

model.start config
