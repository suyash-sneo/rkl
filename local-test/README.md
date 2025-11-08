# How to run kafka and produce 10k messages

- Just run `docker compose up -d` to start Kafka and Zookeeper.
- The topic is random-data


# How to validate 

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic random-data \
  --from-beginning \
  --max-messages 10
```
