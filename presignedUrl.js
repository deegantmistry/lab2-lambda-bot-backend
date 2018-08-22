var AWS = require('aws-sdk');
var s3 = new AWS.S3({
  signatureVersion: 'v4',
});


exports.handler = (event, context, callback) => {
  const url = s3.getSignedUrl('putObject', {
    Bucket: 'mwhackathon',
    Key: 'applications/application.json',
    Expires: 345600,
  });


  callback(null, url);
};