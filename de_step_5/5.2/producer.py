import psycopg2
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

conn = psycopg2.connect(
    dbname="test_db",
    user="admin",
    password="admin",
    host="localhost",
    port=5432
)
cursor = conn.cursor()

cursor.execute("""
    SELECT username, event_type, extract(epoch FROM event_time) 
    FROM user_logins
    WHERE sent_to_kafka = FALSE""")
rows = cursor.fetchall()

for row in rows:
    data = {
        "user": row[0],
        "event": row[1],
        "timestamp": float(row[2])  
    producer.send("user_events", value=data)
    print("Sent:", data)

    cursor.execute("""
        UPDATE user_logins 
        SET sent_to_kafka = TRUE
        WHERE username = %s 
        AND event_type = %s
        AND event_time = to_timestamp(%s)
        """, (row[0], row[1], float(row[2])))

    conn.commit()
    time.sleep(0.5)

cursor.close()
conn.close()
producer.close()
