# SSL Certificates

## Файли в цій папці:
- `server.crt` - публічний SSL сертифікат
- `server.key` - приватний ключ
- `ca.crt` - сертифікат центру сертифікації (якщо потрібно)

## Для створення self-signed сертифікатів:
```bash
openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt -days 365 -nodes
```

## Для отримання корпоративного сертифікату:
Зверніться до IT відділу за SSL сертифікатом для внутрішньої мережі