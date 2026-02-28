import ydb

driver = ydb.Driver(connection_string="grpc://localhost:2135/?database=local")
driver.wait()

try:
    driver.topic_client.drop_topic("topic")
except ydb.SchemeError:
    pass

driver.topic_client.create_topic("topic", consumers=["consumer"])

writer = driver.topic_client.writer("topic")

for batch_index in range(1000):
    print(batch_index)
    messages = []
    for message_index in range(1000):
        messages.append(f"{batch_index}-{message_index}")
    writer.write(messages)
    if batch_index % 50 == 0:
        writer.flush()

writer.close()
driver.stop()
print("OK")
