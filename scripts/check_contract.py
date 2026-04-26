import ccxt

exchange = ccxt.htx({'options': {'defaultType': 'swap'}})
markets = exchange.load_markets()
print(markets['ETH/USDT:USDT']['contractSize'])
print(markets['BTC/USDT:USDT']['contractSize'])
