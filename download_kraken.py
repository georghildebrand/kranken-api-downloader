#/usr/bin/python
import requests as r
import datetime as dt

def nxtime2datetime(nix_time):
    return dt.datetime.fromtimestamp(nix_time)


#get time of server
url = 'https://api.kraken.com/0/public/Time'
resp = r.get(url)
print(resp.json())
resp = resp.json()
server_now = resp['result']['unixtime']
start = server_now - 12*60*60 # substracting half a day in seconds


url = 'https://api.kraken.com/0/public/AssetPairs'
# pair = comma delimited list of asset pairs to get info on
resp = r.get(url).json()
print(resp['result'].keys())
pairs = resp['result'].keys()

#pairs = ['DASHEUR', 'DASHUSD', 'DASHXBT', 'GNOETH', 'GNOEUR', 'GNOUSD', 'GNOXBT', 'USDTZUSD', 'XETCXETH', 'XETCXXBT', 'XETCZEUR', 'XETCZUSD', 'XETHXXBT', 'XETHXXBT.d', 'XETHZCAD', 'XETHZCAD.d', 'XETHZEUR', 'XETHZEUR.d', 'XETHZGBP$
today = dt.datetime.now()
# kraken seems to offer only the last 12 hours for the 1 min interval
#so we need to trigger all 12 h
print('Getting infor for last 12 hours')

for pair in pairs:

    cols = '<time>, <open>, <high>, <low>, <close>, <vwap>, <volume>, <count>'
    cols = cols.replace(" ",'').replace("<","").replace(">","").split(",")
    # since in unixtime 
    url = 'https://api.kraken.com/0/public/OHLC?pair={pair}&interval=1&since={since}'.format(since=start, pair=pair)
    try:
        resp = r.get(url).json()
        result = resp['result'][pair]
        # append to daily file
        with open(today.strftime('%Y-%m-%d-%H-%M')+'-'+pair+'.csv', 'w') as f:
            f.write(",".join(cols)+"pair"+'\n')
            for row in result:
                row[0] = nxtime2datetime(row[0])
                row[0] = row[0].strftime('%Y-%m-%d %H:%M')
                row.append(pair)
                row.append('\n')
                f.write(",".join(map(str, row)))

    except Exception as e:
        print(e)
        continue

