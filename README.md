An example of Glue schema derivation using Magnolia.
The schema is used to create a Glue table + Firehose that converts json into parquet format.
Glue/Firehose and other aws resources are created with [aws-cdk](https://github.com/aws/aws-cdk).

### Setup 
1) `npm install -g aws-cdk`
2) Init cdk (this action is required only once per account+region). 
Execute only if nobody used cdk in your aws account.
``` 
cdk bootstrap --bootstrap-bucket-name cdk-toolkit-bootstrap
```

### Usage

1) See the changes that are going to be applied
```
cdk diff
```
2) Apply the changes
```
cdk deploy
```
3) Send a sample event to firehose
```
aws firehose put-record --delivery-stream-name melgenek_test_firehose --record file://notification-example.json
```
4) Add partitions to the table
```
MSCK REPAIR TABLE melgenek_test.melgenek_test;
```
5) Query Athena
```
SELECT * FROM melgenek_test.melgenek_test limit 10;
```
6) Delete the stack when it is not needed any more
```
aws s3 rm --recursive s3://melgenek-test-firehose
cdk destroy
```
