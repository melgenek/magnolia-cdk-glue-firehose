An example of Glue schema derivation using Magnolia.
The schema is used to create a Glue table + Firehose that converts json into parquet format.
Glue/Firehose and other aws resources are created with [aws-cdk](https://github.com/aws/aws-cdk).

### Setup 
1) `npm install -g aws-cdk`
2) Init cdk (this action is required only once per account+region). 
Execute only if nobody used cdk in your aws account.
``` 
cdk bootstrap --bootstrap-bucket-name cdk-demo-bootstrap
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
sbt "project client" run
```
4) Add partitions to the table
```
MSCK REPAIR TABLE demo.events;
```
5) Query Athena
```
SELECT * FROM demo.events limit 10;
```
6) Delete the stack when it is not needed any more
```
aws s3 rm --recursive s3://demo-purchase-events
cdk destroy
```
