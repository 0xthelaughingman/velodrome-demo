from configparser import ConfigParser
import requests
import json
import time

conf = ConfigParser()
conf.read('config.conf')

# SETUP CONFIGS
API_KEY = conf['default']['api_key']
print(len('00000000000000000000000000000000000000000000000000000000000015c0'))
SQL_QUERY = """
    SELECT 
        block_timestamp,
        tx_hash,
        from_address,
        input_data
        
    from optimism.core.fact_transactions
    WHERE
        input_data LIKE '0x7ac09bf7%'
        AND to_address = '0x09236cff45047dbee6b921e00704bed6d6b8cf7e'
        AND tx_hash = '0x9c912532140845b9a4926a99b6c8dfd38340e09761e1d1ae2eea455d6eeea2af'
"""

TTL_MINUTES = 15

def create_query():
    r = requests.post(
        'https://node-api.flipsidecrypto.com/queries', 
        data=json.dumps({
            "sql": SQL_QUERY,
            "ttlMinutes": TTL_MINUTES
        }),
        headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": API_KEY},
    )
    if r.status_code != 200:
        raise Exception("Error creating query, got response: " + r.text + "with status code: " + str(r.status_code))
    
    return json.loads(r.text)    


def get_query_results(token):
    r = requests.get(
        'https://node-api.flipsidecrypto.com/queries/' + token, 
        headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": API_KEY}
    )
    if r.status_code != 200:
        raise Exception("Error getting query results, got response: " + r.text + "with status code: " + str(r.status_code))
    
    data = json.loads(r.text)
    if data['status'] == 'running':
        time.sleep(10)
        return get_query_results(token)

    return data

def wrap(s, w):
    return [s[i:i + w] for i in range(0, len(s), w)]


'''
MethodID: 0x7ac09bf7
[0]:  0000000000000000000000000000000000000000000000000000000000001077
[1]:  0000000000000000000000000000000000000000000000000000000000000060
[2]:  00000000000000000000000000000000000000000000000000000000000000a0
[3]:  0000000000000000000000000000000000000000000000000000000000000001  -> pool counts
[4]:  00000000000000000000000079c912fef520be002c2b6e57ec4324e260f38e50  -> pools
[5]:  0000000000000000000000000000000000000000000000000000000000000001  -> weight counts
[6]:  0000000000000000000000000000000000000000000000000000000000002710  -> weights
'''


def build_data(data):

    for row in data['results']:
        long_hex = row[3].split('0x7ac09bf7')[1]
        hex_rows = wrap(long_hex, 64)
        #print(hex_rows)

        token_id = int(hex_rows[0], 16)
        pools = []
        weights = []
       
        for i in range(0, int(hex_rows[3], 16)):
            pools.append('0x'+ hex_rows[4 + i][24:])

        for i in range(0, int(hex_rows[3 + len(pools) + 1], 16)):
            weights.append(int(hex_rows[3 + len(pools) + 2 +i], 16))

        #print(pools,weights)

        # build each row as needed for data:
        for i in range(0, len(pools)):
            print(row[0], row[1], row[2], pools[i], weights[i])


def run():
    query = create_query()
    token = query.get('token')
    data = get_query_results(token)

    print(data['columnLabels'])

    build_data(data)


if __name__ == '__main__':
    run()