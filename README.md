Тестовое задание для python-разработчика. Выполнено 19.02.23
========================
-------------------------
***
### Setup:

>Создать тестовое окружение:

    python3 -m venv venv
>Активировать его:

    source venv/bin/activate
>Установить зависимости:

    pip install -r requirements.txt
>Установить ClickHouse и запустить сервер:

    sudo apt-get install -y apt-transport-https ca-certificates dirmngr
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754

    echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee /etc/apt/sources.list.d/clickhouse.list
    sudo apt-get update
<!-- БЕЗ ПАРОЛЯ-->
    sudo apt-get install -y clickhouse-server clickhouse-client

    sudo service clickhouse-server start
>Установить Redis и запустить сервер:

    curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg

    echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list

    sudo apt-get update
    sudo apt-get install redis
    redis-server --daemonize yes
>Запустить скрипт:

    python3 main.py
___
Features
-------------------------
* При проектировании db решил оптимизировать хранение Mac-адресов в UInt64, но такое же поле в очереди Redis передаю в Hex-формате (AA:BB:CC:DD:EE:FF)
