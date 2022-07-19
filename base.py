from logging import exception
import numpy as np
import requests
import json
import time
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# SETUP CONFIGS
# As per your .toml file.
API_KEY = st.secrets.sdk_creds.api_key

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

pool_lookup = {
    '0x4f7ebc19844259386dbddb7b2eb759eefc6f8353':'StableV1 AMM - USDC/DAI',
    '0xd16232ad60188b68076a235c65d692090caba155':'StableV1 AMM - USDC/sUSD',
    '0x79c912fef520be002c2b6e57ec4324e260f38e50':'VolatileV1 AMM - WETH/USDC',
    '0x47029bc8f5cbe3b464004e87ef9c9419a48018cd':'VolatileV1 AMM - OP/USDC',
    '0xfd7fddfc0a729ecf45fb6b12fa3b71a575e1966f':'StableV1 AMM - WETH/sETH',
    '0xd62c9d8a3d4fd98b27caaefe3571782a3af0a737':'StableV1 AMM - USDC/MAI',
    '0xadf902b11e4ad36b227b84d856b229258b0b0465':'StableV1 AMM - FRAX/USDC',
    '0xe8537b6ff1039cb9ed0b71713f697ddbadbb717d':'VolatileV1 AMM - VELO/USDC',
    '0x207addb05c548f262219f6bfc6e11c02d0f7fdbe':'StableV1 AMM - USDC/LUSD',
    '0x587233ce63d7c1e081ce9d94d9940544758f6d01':'VolatileV1 AMM - FRAX/USDC',
    '0xec24eb97cec2f0f6a2d61254990b0f163bbbfe1d':'StableV1 AMM - sUSD/DAI',
    '0x6fd5bee1ddb4dbbb0b7368b080ab99b8ba765902':'StableV1 AMM - alETH/WETH',
    '0x6c5019d345ec05004a7e7b0623a91a0d9b8d590d':'StableV1 AMM - USDC/DOLA',
    '0xe75a3f4bf99882ad9f8aebab2115873315425d00':'StableV1 AMM - USDC/alUSD',
    '0xffd74ef185989bff8752c818a53a47fc45388f08':'VolatileV1 AMM - VELO/OP',
    '0xdee1856d7b75abf4c1bdf986da4e1c6c7864d640':'VolatileV1 AMM - LYRA/USDC',
    '0x43ce87a1ad20277b78cae52c7bcd5fc82a297551':'VolatileV1 AMM - WETH/DOLA',
    '0x9c8a59934fba9af82674eff5d13a24e7c7e7a1f1':'VolatileV1 AMM - USDC/PERP',
    '0x9355292f66552ea5717b274d27eefc8254011d83':'VolatileV1 AMM - THALES/USDC',
    '0x43c3f2d0aa0ebc433d654bb6ebf67f0c03f8d8d9':'VolatileV1 AMM - OP/DAI',
    '0x588443c932b45f47e936b969eb5aa6b5fd4f3369':'VolatileV1 AMM - HND/USDC',
    '0xc2058aa3b3f96075cc33946bcd1963bfa660315b':'VolatileV1 AMM - sUSD/DAI',
    '0xac49498b97312a6716ef312f389b7e4d183a2a7c':'StableV1 AMM - FRAX/sUSD',
    '0xe2ea57fdf87624f4384ef6da5f3844e8e9e5d878':'VolatileV1 AMM - FRAX/FXS',
    '0xcdd41009e74bd1ae4f7b2eecf892e4bc718b9302':'VolatileV1 AMM - WETH/OP',
    '0xffb6c35960b23989037c8c391facebc8a17de970':'VolatileV1 AMM - WETH/SNX',
    '0x85ff5b70de43fee34f3fa632addd9f76a0f6baa9':'VolatileV1 AMM - SNX/sUSD',
    '0x986d353a3700530be4e75794830f57e657bc68cb':'VolatileV1 AMM - FRAX/OP',
    '0xfdad8f85c0f3895c85301f549d124ce526479bf8':'StableV1 AMM - WBTC/renBTC',
    '0x06141423dcf1a5a4c137039063ac873cdc1e363a':'VolatileV1 AMM - VELO/WETH',
    '0x4c8b195d33c6f95a8262d56ede793611ee7b5aad':'VolatileV1 AMM - WBTC/USDC',
    '0xe8633ce5d216ebfdddf6875067dfb8397dedcaf3':'StableV1 AMM - OP/USDC',
    '0x335bd4ffa921160fc86ce3843f80a9941e7456c6':'StableV1 AMM - VELO/USDC',
    '0xe47d437252fe9cb5e74396eee63360d8647df25d':'VolatileV1 AMM - LYRA/sUSD',
    '0x93fc04cd6d108588ecd844c7d60f46635037b5a3':'VolatileV1 AMM - USDC/sUSD',
    '0xfc77e39De40E54F820E313039207DC850E4C9E60':'VolatileV1 AMM - OP/L2DAO',
    '0x53bea2d15efe344b054e73209455d2b6aa1c9462':'VolatileV1 AMM - OP/sUSD'
}

def wrap(s, w):
    return [s[i:i + w] for i in range(0, len(s), w)]

# MethodID: 0x7ac09bf7
# [0]:  0000000000000000000000000000000000000000000000000000000000001077
# [1]:  0000000000000000000000000000000000000000000000000000000000000060
# [2]:  00000000000000000000000000000000000000000000000000000000000000a0
# [3]:  0000000000000000000000000000000000000000000000000000000000000001  -> pool count N
# [4]:  00000000000000000000000079c912fef520be002c2b6e57ec4324e260f38e50  -> N pools
# [5]:  0000000000000000000000000000000000000000000000000000000000000001  -> weight count N
# [6]:  0000000000000000000000000000000000000000000000000000000000002710  -> N weights

def build_data(data):
    output = []
    for row in data['results']:
        long_hex = row[3].split('0x7ac09bf7')[1]
        hex_rows = wrap(long_hex, 64)
        # print(hex_rows)

        # take taoken_id from first row
        token_id = int(hex_rows[0], 16)
        pools = []
        weights = []

        # loop N addresses using the count in row#4
        # get each address, fix the string
        for i in range(0, int(hex_rows[3], 16)):
            pools.append('0x'+ hex_rows[4 + i][24:])

        # use len/count of pools for deducing displacement of rows to get weight count row/weight row start
        # get each weight
        for i in range(0, int(hex_rows[3 + len(pools) + 1], 16)):
            weights.append(int(hex_rows[3 + len(pools) + 2 +i], 16))

        # print(pools,weights)

        # build each tuple as needed for data:
        for i in range(0, len(pools)):
            # print(row[0], row[1], row[2],token_id, pools[i], weights[i])
            # check if address in lookup dict
            try:
                alias = pool_lookup[pools[i]]
            except Exception:
                alias = "NA"

            output.append((row[0], row[1], row[2],token_id, pools[i], alias, weights[i]))
    return output

def run():
    query = create_query()
    token = query.get('token')
    data = get_query_results(token)
    # print(data['columnLabels'])
    
    st.title('Velodrome Voting Stats')
    st.write('''Disclaimer: Since getting accurate voting stats was not possible with Snowflake/Velocity, 
    I have tried to use streamlit and it's supported charting libraries. Since these are a bit new for me, 
    most interactive features are currently lacking D:\n
    INCASE OF EXCEPTIONS/Timeouts RELOAD/REFRESH the page!''')
    
    st.write('''We look at how people people are using their votes, and which pools are the most popular based on\n
    1. Highest Weights/Votes gained\n
    2. Most Distinct Users that voted\n
    3. Most distinct vote transactions casted''')

    op_tuples = build_data(data)
    # print(op_tuples)

    df = pd.DataFrame(op_tuples, columns =['timestamp', 'hash', 'wallet', 'token_id', 'pool_address', 'pool_alias', 'weights'])
    print(df)

    totals_base = df.groupby(['pool_address','pool_alias']).agg(
            total_weight=('weights', np.sum),
            total_txns=('hash', lambda x: x.nunique()),
            total_users=('wallet', lambda x: x.nunique())
        )

    totals = totals_base.sort_values("total_weight", ascending = False).head(10).reset_index()
    # print(totals)

    fig, ax = plt.subplots()
    ax.set_title('Top 10 Pools by weights')
    ax.bar(x=totals['pool_alias'], height=totals['total_weight'])
    plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    st.pyplot(fig)

    totals_wallet = totals_base.sort_values("total_users", ascending = False).head(10).reset_index()
    fig, ax = plt.subplots()
    ax.set_title('Top 10 Pools by Distinct Voters')
    ax.bar(x=totals_wallet['pool_alias'], height=totals_wallet['total_users'])
    plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    st.pyplot(fig)

    totals_txns = totals_base.sort_values("total_txns", ascending = False).head(10).reset_index()
    fig, ax = plt.subplots()
    ax.set_title('Top 10 Pools by Distinct Vote Transactions')
    ax.bar(x=totals_txns['pool_alias'], height=totals_txns['total_txns'])
    plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    st.pyplot(fig)

    st.write("It's rather amusing that the top 10 pools don't change in either of the metrics!!!")

    
if __name__ == '__main__':
    run()