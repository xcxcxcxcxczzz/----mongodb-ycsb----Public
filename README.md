# ----mongodb-ycsb----Public
- build </br>
mvn -pl site.ycsb:mongodb-binding -am clean package
- run </br>
bin/ycsb.bat load mongodb -s -P workloads/workloadb -p recordcount=1000000 -p mongodb.url=mongodb://localhost:27017 -p batchsize=10000 -p linkcount=4 -p enckey=1234567890abcdef -p authkey=abcdef1234567890
#### copy file trong folder client file vào YCSB\mongodb\src\main\java\site\ycsb\db
- mongodb gốc: mongodb/MongoDbClient.java
- thuật toán:  code/MongoDbClient.java
