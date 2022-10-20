# s3-multi-part-copy

The goal of this project is to better understand the low-level S3 MultiPartUpload and UploadPartCopy APIs.

## Development

```
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
python3 cli.py
pytest
```

## Link Dump
- [#aws-cli: Github Issue 2458 [Feature Request] Add tagging support to s3 #2458](https://github.com/aws/aws-cli/issues/2458)
- [teppen.io Blog: All about AWS S3 ETags](https://teppen.io/2018/06/23/aws_s3_etags/)
- [StackOverflow: How do you find the part size used to create an existing multipart object on Amazon S3?](https://stackoverflow.com/questions/45421156/how-do-you-find-the-part-size-used-to-create-an-existing-multipart-object-on-ama)
- [Gist: teasherm/s3_multipart_upload.py](https://gist.github.com/teasherm/bb73f21ed2f3b46bc1c2ca48ec2c1cf5)
- [CoderFlex.co Blog: Python S3 Multipart File Upload with Metadata and Progress Indicator](https://codeflex.co/python-s3-multipart-file-upload-with-metadata-and-progress-indicator/)
- [StackOverflow: AWS Boto3 S3 copy not copying tags](https://stackoverflow.com/questions/60807368/aws-boto3-s3-copy-not-copying-tags)
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
    - [Multipart upload initiation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html)
    - [S3.Client.create_multipart_upload](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.create_multipart_upload)
- [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html)
    - [S3.Client.upload_part_copy](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_part_copy)
- [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
    - [S3.Client.complete_multipart_upload](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.complete_multipart_upload)
- [s3transfer added support for Tagging and TaggingDirective directive in version 0.3.1](https://github.com/boto/s3transfer/blob/develop/CHANGELOG.rst#031)
