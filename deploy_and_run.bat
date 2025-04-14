@echo off
set SERVER=root@172.236.130.176
set LOCAL_PATH=C:\Users\sntfi\Desktop\Stratum\stratum_proxy.py
set REMOTE_PATH=/root/Stratum/stratum_proxy.py

echo 🔄 Копируем новый файл...
scp "%LOCAL_PATH%" %SERVER%:%REMOTE_PATH%

echo 🧹 Завершаем старый процесс...
ssh %SERVER% "pkill -f stratum_proxy.py"

echo 🚀 Запуск новой версии...
ssh %SERVER% "cd /root/Stratum && source venv/bin/activate && nohup python stratum_proxy.py > output.log 2>&1 &"

echo ✅ Готово. Новая версия запущена!
pause
