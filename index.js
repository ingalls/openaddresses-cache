#!/usr/bin/env node

//NPM Dependancies
var download = require('openaddresses-download'),
    argv = require('minimist')(process.argv.slice(2)),
    fs = require('fs'),
    ProgressBar = require('progress'),
    crypto = require('crypto'),
    AWS = require('aws-sdk'),
    time = require('moment'),
    connectors = download.connectors;

//Command Line Args
var sourceDir = argv._[0],
    cacheDir = argv._[1];


var output = "",
    outputName = "",
    parsed = "",
    source = "",
    sourceName = "",
    sourceIndex = 0,
    retry = 0;

if (!sourceDir || !cacheDir) {
    throw new Error('usage: openaddresses-cache <path-to-sources> <path-to-cache>');
}

//Catch missing /
if (sourceDir.substr(sourceDir.length-1) != "/")
    sourceDir = sourceDir + "/";
if (cacheDir.substr(cacheDir.length-1) != "/")
    cacheDir = cacheDir + "/";

//Setup list of sources
var sources = fs.readdirSync(sourceDir);

//Only retain *.json
for (var i = 0; i < sources.length; i++) {
    if (sources[i].indexOf('.json') == -1) {
        sources.splice(i, 1);
        i--;
    }
}

//Begin Downloading Sources
downloadSource(sourceIndex);

//Download Each Source
function downloadSource(index) {
  
    if (index >= sources.length) {
        console.log("Complete!");
        process.exit(0);
    }
    
    var source = sources[index];
    
    this.sourceName = sources[index];
    this.source = sourceDir + source;

    console.log("---" + this.sourceName + "---");
    
    parsed = JSON.parse(fs.readFileSync(sourceDir + source, 'utf8'));

    if (!parsed.data || parsed.skip === true) {
        console.log("   Skipping: Skip=true");
        downloadSource(++sourceIndex);
    } else if (!parsed.type) {
        console.log("   Skipping: No Type");
        downloadSource(++sourceIndex);
    } else if (parsed.type == "ESRI") {
        console.log("   Scraping ESRI Source");

        output = cacheDir + source;
        outputName = source;

        connectors[parsed.type](parsed, function (err, stream) {
            if (err) {
                console.log("   Unable to Stream Data - Skipping");
                downloadSource(++sourceIndex);
            } else {
                var write = fs.createWriteStream(cacheDir + source);
                var addrCount = 0;

                write.on('close', function() {
                    checkHash();
                });

                stream.on('data', function(){
                    process.stdout.write('  Downloaded: ' + ++addrCount + " addresses\r");
                });

                stream.on('error', function(){
                    if (retry != 0){
                        retry++;
                        console.log("   Stream Error! Retry Attempt: " + retry + "/3");
                        downloadSource(sourceIndex);
                    } else {
                        console.log("   Persistant Stream Error - Skipping");
                        downloadSource(++sourceIndex);
                    }
                });

                stream.pipe(write);
            }
        });
    } else if (parsed.type == "http" || parsed.type == "ftp"){
        if (parsed.compression) {
            output = cacheDir + source.replace(".json","") + "." + parsed.compression;
            outputName = source.replace(".json", "") + "." + parsed.compression;
        } else {
            output = cacheDir + source.replace(".json","");
            outputName = source.replace(".json","");
        }

        connectors[parsed.type](parsed, function(err, stream) {

            if (err) {
                console.log("   Unable to Stream Data - Skipping");
                downloadSource(++sourceIndex);
            } else { 

                if (!argv.silent) showProgress(stream, parsed.type);

                var write = fs.createWriteStream(output);

                write.on('close', function() {
                    if (retry == 0) 
                        checkHash();
                });

                stream.pipe(write);

                stream.on('error', function(){
                    if (retry != 0){
                        retry++;
                        console.log("   Stream Error! Retry Attempt: " + retry + "/3");
                        downloadSource(sourceIndex);
                    } else {
                        console.log("   Persistant Stream Error - Skipping");
                        retry = 0;
                        downloadSource(++sourceIndex);
                    }
                });
            }
        });
    } else {
        console.log("   Could not determine download type");
        downloadSource(++sourceIndex);
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

            if (!len)
                console.log("No Size Given By Server - Progress Bar Disabled - Please Be Patient!");
            else {
                bar = new ProgressBar('  downloading [:bar] :percent :etas', {
                    complete: '=',
                    incomplete: '-',
                    width: 20,
                    total: len
                });
            }
        });
    }
    stream.on('data', function(chunk) {
        if (bar) bar.tick(chunk.length);
    }).on('end', function() {

    });
}

function checkHash() {
    retry = 0;
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

function updateManifest() {
    fs.writeFileSync(this.source, JSON.stringify(parsed, null, 4));
    console.log("   Updating Manifest of " + this.source);
}

function updateCache(md5Hash) {
    parsed.fingerprint = md5Hash;
    parsed.version = time().format('YYYYMMDD');
    parsed.cache = "http://s3.amazonaws.com/openaddresses/" + this.sourceName.replace(".json", ".zip");
    
    console.log("   Updating s3 with " + outputName);
    
    var s3 = new AWS.S3();
    fs.readFile(output, function (err, data) {
        if (err)
            throw new Error('Could not find data to upload'); 
        
        var buffer = new Buffer(data, 'binary');

        var s3 = new AWS.S3();
        
        s3.client.putObject({
            Bucket: 'openaddresses',
            Key: outputName,
            Body: buffer,
            ACL: 'public-read'
        }, function (response) {
            console.log('  Successfully uploaded package.');
            updateManifest();
            downloadSource(++sourceIndex);
        });
    });
}
