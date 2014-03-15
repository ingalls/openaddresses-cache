#!/usr/bin/env node

var download = require('openaddresses-download'),
    argv = require('minimist')(process.argv.slice(2)),
    _ = require('underscore'),
    fs = require('fs'),
    ProgressBar = require('progress'),
    MD5 = require('MD5');

var sourceDir = argv._[0],
    cacheDir = argv._[1],
    connectors = download.connectors;

if (!sourceDir || !cacheDir) {
    throw new Error('usage: openaddress-cache path-to-sources path-to-cache');
}

var sources = fs.readdirSync(sourceDir);

//Only Keep json
for (var i = 0; i < sources.length; i++){
  if (sources[i].indexOf('.json') == -1){
    sources.splice(i, 1);
    i--;
  }
}

//Download Each Source
_.each(sources, function(source){
  var parsed = JSON.parse(fs.readFileSync(sourceDir + source, 'utf8'));

  if (!parsed.data) {
      throw new Error('no data included in source');
  }

  var type = connectors.byAddress(parsed.data);

  if (!type) {
      throw new Error('no connector found');
  }

  if (source.compression == undefined)
    source.compression = "";

  connectors[type](parsed, function(err, stream) {
      if (!argv.silent) showProgress(stream, type);
      stream.pipe(fs.createWriteStream(cacheDir + source.replace(".json","") + source.compression));
  });
});

function showProgress(stream, type) {
    var bar;
    if (type == 'http') {
        stream.on('response', function(res) {
            var len = parseInt(res.headers['content-length'], 10);
            bar = new ProgressBar('  downloading [:bar] :percent :etas', {
                complete: '=',
                incomplete: ' ',
                width: 20,
                total: len
            });
        });
    } else if (type == 'ftp') {
        stream.on('size', function(len) {
            bar = new ProgressBar('  downloading [:bar] :percent :etas', {
                complete: '=',
                incomplete: ' ',
                width: 20,
                total: len
            });
        });
    }
    stream.on('data', function(chunk) {
        if (bar) bar.tick(chunk.length);
    }).on('end', function() {
        if (bar) console.log('\n');
    });
}
