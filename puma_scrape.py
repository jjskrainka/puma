"""
Author: Jack Skrainka
PUMA scraper output table PUMA_BRONZE

"""

import requests
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
import os

from pprint import pprint

# define source urls of interest
engiGraduationsByPuma = 'https://datausa.io/api/data?CIP=14&drilldowns=PUMA&measure=Completions'
uniGraduationsByPuma = 'https://datausa.io/api/data?measures=Completions&drilldowns=PUMA,University'
popMetricsByPuma = 'https://datausa.io/api/data?measure=ygcpop%20RCA,Total%20Population,Total%20Population%20'
engiPopMetricsByPuma = 'https://datausa.io/api/data?CIP2=14&measure=ygcpop%20RCA,Total%20Population,Total%20Popul'

# organize them by structure of returned data
popMetrics = [popMetricsByPuma, engiPopMetricsByPuma]
gradMetrics = [engiGraduationsByPuma, uniGraduationsByPuma]


def main():
    # requests used to get raw_data
    grad_datalist = get_data(gradMetrics)
    pop_datalist = get_data(popMetrics)

    # normalize column names across datasets
    df = merge_datasets(grad_datalist, pop_datalist)

    build_table(df)

    return print('Table is ready!')


def get_data(urllist):
    # API data is returned as a JSON containing a list of dictionaries within 'data' key
    # TODO: handle 'source' key for full traceability
    contentlist = []
    for url in urllist:
        content = requests.get(url).json()
        contentlist += content['data']

    return contentlist


def merge_datasets(grads, pop):
    """
    :param grads: list of dictionaries
    :param pop: list of dictionaries
    :return: dataframe
    """
    graddf = pd.DataFrame(grads)

    # extract state abbreviations from PUMA string
    graddf['state'] = [get_state(i) for i in graddf['PUMA']]
    popdf = pd.DataFrame(pop)

    # normalize cols
    popdf['CIP2'] = [i.lower() if i == i else i for i in popdf['CIP2']]
    popdf['Slug CIP'] = popdf['CIP2']

    # merge normalized data
    return pd.merge(graddf, popdf, on=['ID Year', 'Year', 'Slug CIP'])


def build_table(df):
    engine = create_engine(f'sqlite:///puma.db', echo=False)
    df.to_sql('PUMA_BRONZE', con=engine, if_exists='replace')


def get_state(pumastr):
    # list of states except PR and VI here https://gist.github.com/JeffPaine/3083347
    states = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
              'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
              'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
              'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX',
              'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY', 'PR', 'VI']

    state = pumastr[len(pumastr)-2:]
    if state not in states:
        state = None

    return state


if __name__ == "__main__":
    main()
