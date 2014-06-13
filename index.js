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

var winston = require('winston');

winston.add(winston.transports.File, { filename: './logger.log' });

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
    console.log('usage: openaddresses-cache <path-to-sources> <path-to-cache>');
    console.log('       openaddresses-cache  <single source>  <path-to-cache>');
    process.exit(0);
}

if (cacheDir.substr(cacheDir.length-1) != "/")
    cacheDir = cacheDir + "/";

var sources = [];

if (sourceDir.indexOf(".json") != -1) {
    var dir = sourceDir.split("/"),
        singleSource = dir[dir.length-1];

    sourceDir = sourceDir.replace(singleSource,"");
    
    sources.push(singleSource);
} else {
    //Catch missing /
    if (sourceDir.substr(sourceDir.length-1) != "/")
        sourceDir = sourceDir + "/";

    //Setup list of sources
    sources = fs.readdirSync(sourceDir);

    //Only retain *.json
    for (var i = 0; i < sources.length; i++) {
        if (sources[i].indexOf('.json') == -1) {
            sources.splice(i, 1);
            i--;
        }
    }
}

//Begin Downloading Sources
downloadSource(sourceIndex);

//Download Each Source
function downloadSource(index) {
  
    if (index >= sources.length) {
        winston.info("Complete!");
        process.exit(0);
    }
    
    var source = sources[index];
    
    this.sourceName = sources[index];
    this.source = sourceDir + source;

    winston.info("---" + this.sourceName + "---");
    
    parsed = JSON.parse(fs.readFileSync(sourceDir + source, 'utf8'));

    if (!parsed.data || parsed.skip === true) {
        winston.info("   Skipping: Skip=true");
        downloadSource(++sourceIndex);
    } else if (!parsed.type) {
        winston.info("   Skipping: No Type");
        downloadSource(++sourceIndex);
    } else if (parsed.type == "ESRI") {
        winston.info("   Scraping ESRI Source");

        output = cacheDir + source;
        outputName = source;

        connectors[parsed.type](parsed, function (err, stream) {
            if (err) {
                winston.info("   Unable to Stream Data - Skipping");
                downloadSource(++sourceIndex);
            } else {
                var write = fs.createWriteStream(cacheDir + source);
                var addrCount = 0;

                write.on('close', function() {
                    checkHash();
                });

                stream.on('data', function(){
                    process.stdout.write('   Downloaded: ' + ++addrCount + " addresses\r");
                });

                stream.on('error', function(){
                    if (retry < 3){
                        ++retry;
                        winston.info("   Stream Error! Retry Attempt: " + retry + "/3");
                        downloadSource(sourceIndex);
                    } else {
                        winston.info("   Persistant Stream Error - Skipping");
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
                winston.info("   Unable to Stream Data - Skipping");
                downloadSource(++sourceIndex);
            } else { 

                if (!argv.silent) showProgress(stream, parsed.type);

                var write = fs.createWriteStream(output);

                write.on('close', function() {
                    if (retry === 0) 
                        checkHash();
                });

                stream.pipe(write);

                stream.on('error', function(){
                    if (retry < 3){
                        ++retry;
                        winston.info("   Stream Error! Retry Attempt: " + retry + "/3");
                        downloadSource(sourceIndex);
                    } else {
                        winston.info("   Persistant Stream Error - Skipping");
                        retry = 0;
                        downloadSource(++sourceIndex);
                    }
                });
            }
        });
    } else {
        winston.info("   Could not determine download type");
        downloadSource(++sourceIndex);
    }
}

function showProgress(stream, type) {
    var bar;
    if (type == 'http') {
        stream.on('response', function(res) {
            var len = parseInt(res.headers['content-length'], 10);

            if (len) {            
                bar = new ProgressBar('   downloading [:bar] :percent :etas', {
                    complete: '=',
                    incomplete: '-',
                    width: 20,
                    total: len
                });
            } else {
                winston.info("   No Size Given By Server - Progress Bar Disabled - Please Be Patient!");
            }
        });
    } else if (type == 'ftp') {
        stream.on('size', function(len) {

            if (len)
                bar = new ProgressBar('   downloading [:bar] :percent :etas', {
                    complete: '=',
                    incomplete: '-',
                    width: 20,
                    total: len
                });
            else {
                winston.info("   No Size Given By Server - Progress Bar Disabled - Please Be Patient!");
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
    winston.info("   Updating Manifest of " + this.source);
}

function updateCache(md5Hash) {
    parsed.fingerprint = md5Hash;
    parsed.version = time().format('YYYYMMDD');
    parsed.cache = "http://s3.amazonaws.com/openaddresses/" + parsed.version + "/" + outputName;
    
    winston.info("   Updating s3 with " + outputName);
    
   var s3 = new AWS.S3.Client(),
       versioned = 'openaddresses/' + parsed.version;

    s3.headBucket({Bucket:versioned},function(err, data) {
        if (err) {
            s3.createBucket({Bucket: versioned}, function(err, data) {
                if (err) throw new Error("Could not create bucket");
            });
        }
        
        var Uploader = require('s3-streaming-upload').Uploader,
            upload = null,
            stream = fs.createReadStream(output);
        
        upload = new Uploader({
            accessKey:  process.env.AWS_ACCESS_KEY_ID,
            secretKey:  process.env.AWS_SECRET_ACCESS_KEY,
            bucket:     "openaddresses/" + parsed.version,
            objectName: outputName,
            stream:     stream,
            objectParams: {
                ACL: 'public-read'
            }
        });

        upload.on('completed', function (err, res) {
            console.log('   Successfully uploaded package.');
            updateManifest();
            downloadSource(++sourceIndex);
        });

        upload.on('failed', function (err) {
            console.log('upload failed with error', err);
            downloadSource(++sourceIndex);
        }); 
    });
}
