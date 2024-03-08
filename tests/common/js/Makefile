JS_SOURCES := $(wildcard *.js);

data: node_modules $(JS_SOURCES)
	yarn node index.js

node_modules: package.json
	yarn install
