import json
import urllib3
import os
from ipaddress import ip_address
from multiprocessing import Pool, cpu_count
from random import randint
from time import sleep

import clickhouse_connect
from clickhouse_connect.driver.exceptions import OperationalError
from pbwrap import Pastebin
from random_username.generate import generate_username
from redis.client import Redis

with open('config.json') as config_json:
    config = json.load(config_json)

API_PB_KEY = config['API_PB_KEY']
PB_USERNAME = config['API_PB_KEY']
PB_PASSWORD = config['API_PB_KEY']

CPU_LOAD = 3
NUMBER_DATA_CH = 1000
TABLE_NAME = 'test_table'
QUEUE_NAME = 'tasks'
TIMEOUT_QUEUE = 0
OUT_FILE = 'result.txt'
PATH_TO_FILE = f'{os.getcwd()}/{OUT_FILE}'


def generate_rand_ipv4s(n):
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
    usernames = generate_username(NUMBER_DATA_CH)
    ipv4s = generate_rand_ipv4s(NUMBER_DATA_CH)
    int_macs = generate_randint_macs(NUMBER_DATA_CH)
    data = [*zip(usernames, ipv4s, int_macs)]
    return data


def get_clickhouse_client():
    ch_client = None
    while ch_client is None:
        try:
            ch_client = clickhouse_connect.get_client(host='localhost')
        except (urllib3.exceptions.MaxRetryError, OperationalError):
            print('ClickHouse недоступен')
            sleep(60)

    return ch_client


def create_ch_test_table(ch_client, data):
    ch_client.command(f'DROP TABLE IF EXISTS {TABLE_NAME}')
    ch_client.command(
        'CREATE TABLE test_table (username String, ipv4 IPv4, mac UInt64)'
        'ENGINE MergeTree ORDER BY username'
    )
    ch_client.command('SHOW DATABASES')
    ch_client.insert('test_table', data, column_names=['username', 'ipv4', 'mac'])


def get_pastebin_client():
    pb_client = None
    while pb_client is None:
        try:
            pb_client = Pastebin(API_PB_KEY)
            pb_client.authenticate(PB_USERNAME, PB_PASSWORD)
        except urllib3.exceptions.MaxRetryError:
            print('Pastebin недоступен')
            sleep(60)

    return pb_client


def is_valid_ip(ip_string):
    if not isinstance(ip_string, str):
        return False
    try:
        return bool(ip_address(ip_string))
    except ValueError:
        return False


def is_valid_mac(mac_string, sep=':'):
    if not isinstance(mac_string, str):
        return False
    return int(mac_string.replace(sep, ''), 16) <= 16 ** 12


def query_handler():
    ch_client = get_clickhouse_client()

    with Redis() as redis_client:
        _, task = redis_client.brpop(QUEUE_NAME, timeout=TIMEOUT_QUEUE)
        task = json.loads(task)

    ipv4, mac = task.get('ipv4'), task.get('mac')

    if is_valid_ip(ipv4) and is_valid_mac(mac):  # Разобраться с валидаторами CH
        result = ch_client.query(
            f'SELECT username FROM test_table '
            f'WHERE ipv4 = {ipv4} AND mac = MACStringToNum({mac})'
        ).result_set
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
        with open(PATH_TO_FILE, 'a', encoding='utf-8') as result_file:
            result_file.write(url + '\n')
    return


if __name__ == '__main__':
    create_ch_test_table(get_clickhouse_client(), generate_ch_data())

    with Pool(cpu_count() * CPU_LOAD) as pool:
        while True:
            pool.apply_async(
                query_handler,
                args=tuple(),
                callback=send_json_on_pastebin
            )
        
