# openaddresses-cache

Updates the openaddresses cache. This tool is currently specific to the official openaddresses s3 bucket and therefore can only be updated 
by those who have access to both the access and secret keys.

## usage

    node index.js <source directory> <temp directory> [options]

### Options:

--aws Looks for s3 credentials in environmental variables. See 
http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html
