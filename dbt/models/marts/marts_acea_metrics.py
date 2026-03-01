from logging import log


def model(dbt, session):

    import pandas as pd
    import os

    # read data
    df = dbt.ref('int_acea_data').to_pandas()
   
    # changing unit col to numeric
    df['DATE'] = pd.to_datetime(df['MONTH'], format='%b-%y') + pd.offsets.MonthEnd(0)
    df.drop(columns=['MONTH'], inplace=True)

    # removing YTD rows - will re-calculate
    df_filtered = df[df['FREQUENCY'] != 'YTD']

    # resampling
    df_filtered.set_index('DATE', inplace=True)
    df_filtered = df_filtered.groupby(['MANUFACTURER', 'FREQUENCY', 'REGION']).resample('M')['UNITS'].mean().ffill().reset_index()

    # concating
    to_ret = df_filtered.copy()

    # calculating YTD values
    df_ytd = to_ret.copy()
    df_ytd['Year'] = df_ytd['DATE'].dt.year
    df_ytd['YTD'] = df_ytd.groupby(['MANUFACTURER', 'FREQUENCY', 'REGION', 'Year'])['UNITS'].cumsum()
    df_ytd =  df_ytd.drop(columns=['Year'])

    # calculating rolling sum
    df_ttm = to_ret.copy()
    df_ttm = df_ttm.sort_values(['MANUFACTURER', 'FREQUENCY', 'REGION', 'DATE']).reset_index(drop=True)
    df_ttm['TTM'] = df_ttm.groupby(['MANUFACTURER', 'FREQUENCY', 'REGION'], sort=False)['UNITS'].transform(
        lambda x: x.rolling(window=12, min_periods=1).sum()
    )

    # concating YTD and TTM cuts back to main df
    to_ret['TTM'] = df_ttm['TTM']
    to_ret['YTD'] = df_ytd['YTD']

    # calculate PoP growth rate for each cut
    measures = ['UNITS', 'YTD', 'TTM']
    to_ret[[f'{m}_PoP' for m in measures]] = to_ret.groupby(['MANUFACTURER', 'FREQUENCY', 'REGION'])[measures].pct_change(fill_method=None)

    # calculating yearly growth rate for q and m cuts
    yoy = to_ret.groupby(
        ['MANUFACTURER', 'REGION', 'FREQUENCY'],
        group_keys=False,
    )[measures].apply(lambda g: g.pct_change(12 if g.name[-1] == 'M' else 4, fill_method=None))

    to_ret[[f'{m}_YoY' for m in measures]] = yoy

    # melting
    df_melted = pd.melt(to_ret,
                        id_vars = ['MANUFACTURER', 'REGION', 'FREQUENCY', 'DATE'],
                        value_vars = ['UNITS', 'YTD', 'TTM', 'UNITS_PoP', 'YTD_PoP', 'TTM_PoP', 'UNITS_YoY', 'YTD_YoY', 'TTM_YoY'],
                        var_name = 'Measure',
                        value_name = 'Value')
    
    return df_melted