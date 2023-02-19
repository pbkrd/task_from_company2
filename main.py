import json
import multiprocessing
from random import randint

import clickhouse_connect
from pbwrap import Pastebin
from random_username.generate import generate_username
from redis.client import Redis

with open('config.json') as config_json:
    config = json.load(config_json)

API_PB_KEY = config['API_PB_KEY']
PB_USERNAME = config['API_PB_KEY']
PB_PASSWORD = config['API_PB_KEY']

CPU_LOAD = 3
NUMBER_CH = 1000
PATH_RESULT = 'result.txt'


def generate_rand_ipv4s(n) -> list:
    # ips = ['.'.join(str(randint(0, 255)) for _ in range(4)) for _ in range(n)]
    ips = []
    for _ in range(n):
        ip = '.'.join(str(randint(0, 255)) for _ in range(4))
        ips.append(ip)
    return ips


def generate_randint_macs(n):
    limit = 16 ** 12
    int_macs = [randint(0, limit) for _ in range(n)]
    return int_macs


def generate_ch_data():
    usernames = generate_username(NUMBER_CH)
    ipv4s = generate_rand_ipv4s(NUMBER_CH)
    int_macs = generate_randint_macs(NUMBER_CH)
    data = [*zip(usernames, ipv4s, int_macs)]
    return data


def get_clickhouse_client(query_context=False):
    ch_client = clickhouse_connect.get_client(host='localhost')

    if not query_context:
        return ch_client

    qc = ch_client.create_query_context(
        query='SELECT username FROM test_table WHERE ipv4 = {ipv4:IPv4} AND mac = MACStringToNum({mac:String})',
        parameters={'ipv4': None, 'mac': None},
        column_oriented=True
    )
    return ch_client, qc


def create_ch_test_table(ch_client, data):
    ch_client.command('DROP TABLE IF EXISTS test_table')  # Можно добавить название table in config
    ch_client.command(
        'CREATE TABLE test_table (username String, ipv4 IPv4, mac UInt64) ENGINE MergeTree ORDER BY username')
    ch_client.command('SHOW DATABASES')
    ch_client.insert('test_table', data, column_names=['username', 'ipv4', 'mac'])


def get_pastebin_client():
    pb = Pastebin(API_PB_KEY)
    pb.authenticate(PB_USERNAME, PB_PASSWORD)
    return pb


def query_handler(clickhouse_cl, query_context):
    with Redis() as redis_client:
        task = json.loads(redis_client.rpop('tasks'))

    ipv4, mac = task.get('ipv4'), task.get('mac')

    if isinstance(ipv4, str) and isinstance(mac, str):
        qc.set_parameters({'ipv4': ipv4, 'mac': mac})
        result = clickhouse_cl.query(context=query_context).result_set
        if result:
            report = {"username": result[0][0], "ipv4": ipv4, "mac": mac}
            return report


def send_json_on_pastebin(response):
    if response:
        report_json = json.dumps(response)
        pb = get_pastebin_client()
        url = pb.create_paste(
            report_json,
            api_paste_private=1,
            api_paste_expire_date='2M',
            api_paste_format='json'
        )
        with open(PATH_RESULT, 'a', encoding='utf-8') as result_file:
            result_file.write(url)
    return


if __name__ == '__main__':
    ch_client, qc = get_clickhouse_client(query_context=True)
    create_ch_test_table(ch_client, generate_ch_data())

    while True:
        with multiprocessing.Pool(multiprocessing.cpu_count() * CPU_LOAD) as pool:
            pool.apply_async(
                query_handler,
                args=(ch_client, qc),
                callback=send_json_on_pastebin
            )


# Сделать проверку ip и Mac через шаблон re или перехватывать ошибку некорректных данных
# Ошибки, корнер кейсы и т.п.
# Очереди redis
# readme.md
# залить на github
# Тестирование на WSL
