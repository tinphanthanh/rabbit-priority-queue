# rabbit-priority-queue

Queue quản lý từng item bằng RabbitMQ (priority 0-10), UI đổi priority, source Node.js.

## Chạy

```
docker compose up --build
```

- UI: http://localhost:3000
- RabbitMQ Management: http://localhost:15672 (guest / guest)

Đổi priority khi item còn `queued`: app publish lại message với priority mới, message cũ bị bỏ qua nhờ `version`.
Thời gian xử lý mỗi item chỉnh qua biến `PROCESS_MS` trong `docker-compose.yml`.
