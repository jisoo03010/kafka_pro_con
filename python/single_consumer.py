from kafka import KafkaConsumer
from json import loads

# topic, broker list
consumer = KafkaConsumer(
    'test',
    # 카프카 클러스터들의 호스트와 포트 정보 리스트
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group', # 컨슈머 그룹을 식별하기 위한 용도
    # producer에서 value를 직렬 했기 때문에 사용
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     consumer_timeout_ms=1000
)

# consumer list를 가져온다
print('[begin] get consumer list')
for message in consumer:
    print("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s" % (
        message.topic, message.partition, message.offset, message.key, message.value
    ))
print('[end] get consumer list')