const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');

const s3 = new AWS.S3({
    accessKeyId: 'xxxxxx',
    secretAccessKey: 'xxxxxx',
    region: 'xxxxx'
});

const localFolderPath = '/home/dell/Downloads/vdocs';
const bucketName = 'goicar';

const uploadFileToS3 = (filePath, bucket, key) => {
    const fileStream = fs.createReadStream(filePath);
    const params = {
        Bucket: bucket,
        Key: key,
        Body: fileStream,
        ContentType: 'application/octet-stream'
    };
    return s3.upload(params).promise();
};

const migrateImages = async () => {
    try {
        const files = fs.readdirSync(localFolderPath);
        for (const file of files) {
            const filePath = path.join(localFolderPath, file);
            if (fs.lstatSync(filePath).isFile()) {
                const s3Key = `vehicaldocs/${file}`;
                console.log(`Uploading ${file} to S3...`);
                await uploadFileToS3(filePath, bucketName, s3Key);
                console.log(`Uploaded ${file} successfully.`);
            }
        }
        console.log('All files have been migrated to S3.');
    } catch (error) {
        console.error('Error migrating files:', error);
    }
};

migrateImages();
