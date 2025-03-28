# 🚀 Stratum Proxy для Antminer S19

Python-прокси с фильтрацией extranonce2, готовый для запуска через Docker.

## 🔧 Установка

```bash
git clone https://github.com/aset-durmagambet/Stratum.git
cd Stratum
docker build -t stratum-proxy .
docker run -d --restart unless-stopped -p 3333:3333 stratum-proxy
```

## ⚙️ Подключение ASIC

```
stratum+tcp://<ТВОЙ_IP_СЕРВЕРА>:3333
```