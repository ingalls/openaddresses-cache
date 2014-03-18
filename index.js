#!/usr/bin/env node

var download = require('openaddresses-download'),
    argv = require('minimist')(process.argv.slice(2)),
    fs = require('fs'),
    ProgressBar = require('progress'),
    crypto = require('crypto'),
    AWS = require('aws-sdk'),
    time = require('moment');

var sourceDir = argv._[0],
    cacheDir = argv._[1],
    connectors = download.connectors;

var output = "",
    parsed = "",
    source = "",
    sourceName = "",
    sourceIndex = 0;

if (!sourceDir || !cacheDir) {
    throw new Error('usage: openaddress-cache <path-to-sources> <path-to-cache>');
}

var sources = fs.readdirSync(sourceDir);

//Only Keep json
for (var i = 0; i < sources.length; i++){
  if (sources[i].indexOf('.json') == -1){
    sources.splice(i, 1);
    i--;
  }
}
downloadSource(sourceIndex);

//Download Each Source
function downloadSource(index){
  if (index < sources.length)
    var source = sources[index];
  else {
    console.log("Complete!");
    process.exit();
  }
  
  this.sourceName = sources[index];
  this.source = sourceDir + source;
  
  parsed = JSON.parse(fs.readFileSync(sourceDir + source, 'utf8'));

  if (!parsed.data | parsed.skip == true) {
      console.log("Skipping: " + this.source);
      downloadSource(++sourceIndex);
  } else if (parsed.data.search(/\/MapServer\/\d+/) !== -1 | parsed.data.search(/\/FeatureServer\/\d+$/) !== -1){
      //TODO Remove once ESRI is supported
      console.log("Skipping ESRI Source: " + this.source);
      downloadSource(++sourceIndex);
  } else {
    console.log("Downloading: " + this.source);

    var type = connectors.byAddress(parsed.data);

    if (!type) {
        throw new Error('no connector found');
    }

    if (parsed.compression != undefined)
      output = cacheDir + source.replace(".json","") + "." + parsed.compression;
    else 
      output = cacheDir + source.replace(".json","");

    connectors[type](parsed, function(err, stream) {
        if (!argv.silent) showProgress(stream, type);
        stream.pipe(fs.createWriteStream(output));
    });
  }
}


function showProgress(stream, type) {
    var bar;
    if (type == 'http') {
        stream.on('response', function(res) {
            var len = parseInt(res.headers['content-length'], 10);
            bar = new ProgressBar('  downloading [:bar] :percent :etas', {
                complete: '=',
                incomplete: '-',
                width: 20,
                total: len
            });
        });
    } else if (type == 'ftp') {
        stream.on('size', function(len) {
            bar = new ProgressBar('  downloading [:bar] :percent :etas', {
                complete: '=',
                incomplete: '-',
                width: 20,
                total: len
            });
        });
    }
    stream.on('data', function(chunk) {
        if (bar) bar.tick(chunk.length);
    }).on('end', function() {
        if (bar) console.log('\n');
        checkHash(output);
    });
}

function checkHash(output){
  
  var fd = fs.createReadStream(output);
  var hash = crypto.createHash('md5');
  hash.setEncoding('hex');

  fd.on('end', function() {
    hash.end();
    var md5Hash = hash.read();
    
    if (parsed.fingerprint != md5Hash)
      updateCache(md5Hash);
    else {
      fs.unlinkSync(output);
      downloadSource(++sourceIndex);
    }
  });

  fd.pipe(hash);
}

function updateManifest(){
  fs.writeFileSync(this.source, JSON.stringify(parsed, null, 4));
  console.log("  Updating Manifest of " + this.source);
}

function updateCache(md5Hash){
  parsed.fingerprint = md5Hash;
  parsed.version = time().format('YYYYMMDD');
  parsed.cache = "http://s3.amazonaws.com/openaddresses/" + this.sourceName.replace(".json", ".zip");
  
  console.log("  Updating s3 with " + this.source);
  
  var s3 = new AWS.S3();
  fs.readFile(output, function (err, data) {
    if (err)
      throw new Error('Could not find data to upload'); 
    
    var buffer = new Buffer(data, 'binary');

    var s3 = new AWS.S3();
    
    s3.client.putObject({
      Bucket: 'openaddresses',
      Key: this.sourceName.replace(".json", ".zip"),
      Body: buffer,
      ACL: 'public-read'
    }, function (response) {
      console.log('  Successfully uploaded package.');
      updateManifest();
      downloadSource(++sourceIndex);
    });
  });

}
