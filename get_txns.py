import pandas as pd
from pandas.io.json import json_normalize
import requests
import datetime
import numpy as np
import config


def get_json_df(url, expand_col):
    resp = requests.get(url)
    data = resp.json()
    df = json_normalize(data, [[expand_col]])
    return df

#Ethereum
def get_eth(address):
    actions = ['tokentx', 'txlist', 'txlistinternal']
    compile_df = pd.DataFrame()
    for action in actions:
        base_url = 'http://api.etherscan.io/api?module=account&action'
        url = '{}={}&address={}&startblock=0&endblock=999999999&sort=asc&apikey={}'.format(
                base_url, action, address, config.etherscan_api_key)
        df = get_json_df(url, 'result')
        compile_df = compile_df.append(df, sort=True)

    #replace whitespace with np.nan
    compile_df = compile_df.apply(lambda x: x.str.strip()).replace('', np.nan)

    #cleaning up some numbers because eth is stupid
    compile_df['tokenDecimal'] = compile_df['tokenDecimal'].apply(lambda row: 18
                                        if pd.isnull(pd.to_numeric(row, errors='coerce')) else row).astype(int)

    #fix the stupid fucking numbers in the
    for val in ['gasPrice', 'value']:
        compile_df[val] = compile_df.apply(lambda row: float(row[val])/int(str(1).ljust(row['tokenDecimal']+1, '0')), axis=1)

    #how you calculate the amount of eth used
    compile_df['gasTotal'] = compile_df['gasUsed'].astype(float) * compile_df['gasPrice'].astype(float)

    #sort and drop duplicate on hash because token pull will have identical hash,
    compile_df['timeStamp'] = compile_df['timeStamp'].apply(lambda row: str(datetime.datetime.fromtimestamp(int(row))))
    compile_df = compile_df.sort_values(by=['timeStamp', 'contractAddress'])
    compile_df = compile_df.drop_duplicates(subset=['hash'], keep='first')

    #make blank contract addresses Ethereum
    compile_df['query'] = address

    #make contract address Ethereum if contract address is null
    compile_df['contractAddress'] = compile_df['contractAddress'].apply(lambda row: 'Ethereum' if pd.isna(row) else row)

    #Revelant Columns, exclude these two lines to troubleshoot
    rev_cols = ['hash', 'timeStamp', 'from', 'to', 'contractAddress', 'value', 'gasTotal', 'query', 'in_out']
    compile_df = compile_df.reindex(columns=rev_cols)

    compile_df.to_csv(config.test_fname, index=False)


get_eth(config.test_eth_addy)
