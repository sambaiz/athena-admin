import AWS from 'aws-sdk';
const s3 = new AWS.S3({apiVersion: '2017-05-18'});

export const listObject = (bucket, prefix) => {
  const params = {
    Bucket: bucket,
    Prefix: prefix,
  };
  return s3.listObjectsV2(params).promise();
};

export const getObject = (bucket, key) => {
  const params = {
    Bucket: bucket,
    Key: key,
  };
  return s3.getObject(params).promise();
};

export const putObject = (bucket, key, data) => {
  const params = {
    Body: data,
    Bucket: bucket,
    Key: key,
  };
  return s3.putObject(params).promise();
};

export const deleteObject = (bucket, key) => {
  const params = {
    Bucket: bucket,
    Key: key,
  };
  return s3.deleteObject(params).promise();
};

export const copyObject = (bucket, src, destKey) => {
  const params = {
    Bucket: bucket,
    CopySource: src,
    Key: destKey,
  };
  return s3.copyObject(params).promise();
};

const locationRegexp = /^s3:\/\/(.*?)\/(.*)/;

export const splitS3Location = (location) => {
  const ma = location.match(locationRegexp);
  if (!ma || ma.length !== 3) {
    throw new Error(`invalid S3 Location ${location}`);
  }
  return {
    bucket: ma[1],
    path: ma[2],
  };
};
