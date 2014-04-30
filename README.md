# openaddresses-cache

Updates the openaddresses cache. This tool is currently specific to the official openaddresses s3 bucket and therefore can only be updated 
by those who have access to both the access and secret keys.

## usage

    node index.js <source directory> <working directory>
    
Where

`<source directory>` contains a complete or partial list of sources from the openaddress project. (Note, if skip = true, then the cache will skip updating these files.

`<working directory>` is an *empty* directory where the files will be downloaded to and then checked against the s3 cache for updats.

## AWS

The script depends on environmental variables being set to store S3 creds. These variables must be called `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
