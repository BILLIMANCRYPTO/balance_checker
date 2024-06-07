import pandas as pd
import requests
from web3 import Web3
import random
from requests.exceptions import ProxyError, ConnectionError
from retrying import retry
from concurrent.futures import ThreadPoolExecutor, as_completed
# Инициализация Web3
web3 = Web3(Web3.HTTPProvider('https://rpc.ankr.com/arbitrum'))

# Чтение прокси из файла
def parse_proxy(proxy):
    parts = proxy.split(':')
    if len(parts) == 4:
        ip = parts[0]
        port = parts[1]
        username = parts[2]
        password = parts[3]
        proxy_address = f"socks5://{username}:{password}@{ip}:{port}"
        return {
            'http': proxy_address,
            'https': proxy_address
        }
    else:
        raise ValueError("Incorrect proxy format. Expected format is IP:port:username:password")

with open('proxies.txt', 'r') as file:
    proxies_list = [parse_proxy(line.strip()) for line in file]

# Функция для проверки баланса кошелька с повторными попытками
@retry(stop_max_attempt_number=5, wait_fixed=2000)
def check_wallet_balance(wallet_address):
    url = f"https://api.rabby.io/v1/user/total_balance?id={wallet_address}"
    while proxies_list:
        proxy = random.choice(proxies_list)
        try:
            response = requests.get(url, proxies=proxy, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return {'wallet_address': wallet_address, 'balance': data['total_usd_value']}
        except (ProxyError, ConnectionError):
            proxies_list.remove(proxy)
            if not proxies_list:
                print("All proxies failed.")
                raise
    return None

# Чтение адресов кошельков из файла
wallet_addresses = []
with open('wallets.txt', 'r') as file:
    wallet_addresses = [web3.to_checksum_address(line.strip()) for line in file]

# Инициализация списка для хранения балансов кошельков
wallet_balances_dict = {wallet: None for wallet in wallet_addresses}

# Функция для выполнения в потоке
def process_wallet(wallet):
    try:
        return check_wallet_balance(wallet)
    except Exception as e:
        print(f'Failed to retrieve balance for wallet {wallet}: {e}')
        return None

# Использование ThreadPoolExecutor для многопоточности
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(process_wallet, wallet): wallet for wallet in wallet_addresses}
    for future in as_completed(futures):
        wallet = futures[future]
        result = future.result()
        if result is not None:
            wallet_balances_dict[wallet] = result['balance']
            print(f'Wallet: {result["wallet_address"]} - Balance: {result["balance"]} USD')

# Создание списка из словаря для сохранения порядка
wallet_balances = [{'wallet_address': wallet, 'balance': balance} for wallet, balance in wallet_balances_dict.items() if balance is not None]

# Создание DataFrame из балансов кошельков
wallet_df = pd.DataFrame(wallet_balances)

# Вычисление общего баланса
total_balance = wallet_df['balance'].sum()

# Сохранение результатов в файл CSV и установление ширины столбцов
with pd.ExcelWriter('balances.xlsx', engine='xlsxwriter') as writer:
    wallet_df.to_excel(writer, sheet_name='Sheet1', index=False)
    worksheet = writer.sheets['Sheet1']
    for idx, col in enumerate(wallet_df.columns):
        max_len = wallet_df[col].astype(str).map(len).max()
        worksheet.set_column(idx, idx, max_len + 2)

# Логирование общей суммы балансов всех кошельков
print(f'Total USD Value of all wallets: {total_balance}')
print('Results saved to balances.xlsx')