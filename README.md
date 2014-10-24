# openaddresses-cache

Updates the openaddresses cache in a remote S3 bucket.

## usage

    node index.js <source directory> <working directory> [<bucket name>]
    node index.js <source> <working directory> [<bucket name>]
    
Where

`<source>` is a single source from the openaddress project.

`<source directory>` contains a complete or partial list of sources from the openaddress project. (Note, if skip = true, then the cache will skip updating these files.

`<working directory>` is an *empty* directory where the files will be downloaded to and then checked against the s3 cache for updates.

`<bucket name>` is an S3 bucket to which you have write permissions.

## AWS

The script depends on environmental variables being set to store S3 creds. These variables must be called `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
