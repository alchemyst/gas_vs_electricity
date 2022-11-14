import pandas as pd
import sqlite3

oldurl = 'https://www.sapia.org.za/old-fuel-prices/'
newurl = 'https://www.sapia.org.za/fuel-prices/'

print('Reading websites')
dfs = (
    pd.read_html(oldurl) 
    + pd.read_html(newurl, decimal=',', thousands=' ')
    # note on 2022-11-06 the "new" url used commas while the old one used periods
    # also note you need to specify both thousands and decimal for it to parse correctly
)

print('Parsing')
parsed = []
for df in dfs:
    try:
        year = int(df.columns[0].strip()[-4:])
        good_df = df
    except:
        continue

    df.columns = ['amount'] + list(df.columns[1:])
    df['year'] = year
    df['region'] = df[df['amount'].str.strip().isin(('COASTAL', 'GAUTENG'))]['amount']
    df['region'] = df['region'].fillna(method='ffill')
    df = df.dropna(axis='columns', how='all').dropna().copy()
    df['amount'] = df['amount'].apply(lambda x: x.split(' *')[0].strip())
    df = df.melt(id_vars=['year', 'region', 'amount'], var_name='month', value_name='cost')
    parsed.append(df)

costs = pd.concat(parsed)
costs['date'] = pd.to_datetime(costs.apply(lambda r: f'{" ".join(r["month"].split(" ")[:2])} {r["year"]}', axis=1))

costs['cost'] = costs['cost'].astype(str).str.replace(' ', '').replace(',', '.').astype(float)
costs = (
    costs
    .drop(columns=['year', 'month'])
    .set_index(['region', 'amount', 'date'])
)

print('Writing to costs.db')
con = sqlite3.connect('costs.db')
costs.to_sql('fuel', con, if_exists='replace')