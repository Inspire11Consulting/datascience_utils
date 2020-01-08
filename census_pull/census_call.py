import os
import pandas as pd
from census import Census
from os.path import join, dirname
from dotenv import load_dotenv


def get_census_api_key():
    """Retrieves census api key from .env file
    """
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)

    api_key = os.getenv('CENSUS_API_KEY')

    return api_key


def retrieve_census_data(area_df, var_list, year_min, year_max, acs=5):
    """Method for batch retreiving data from the census API.

    Parameters
    ----------
    area_df : pandas.DataFrame
        dataframe with state and county FIPs
    var_list : list
        list of strings corresponding to census variable names
    year_min : first year to retureve
        list of ints corresponding to the years of interest
    api_key : string
        private census api key value
    acs : int, 1 or 5
        indicates the number of years estimated(?)

    Outputs
    -------
    census_vars : pandas.DataFrame
        Dataframe with census values for the variables of interest from the
        counties & states of interest
    """
    api_key = get_census_api_key()
    c = Census(api_key)
    census_vars = pd.DataFrame()
    state_list = area_df['stateFips'].unique()
    if acs == 1:
        acs = c.acs1
    elif acs == 5:
        acs = c.acs5

    for state in state_list:
        census_dict = acs.state_county(var_list, state, Census.ALL, year=year)
        census_df = pd.DataFrame.from_dict(census_dict)
        counties = area_df[area_df['stateFips'] == state][['countyFips']]
        census_df = census_df.merge(counties,
                                    how='inner',
                                    left_on='county',
                                    right_on='countyFips')

        if len(census_vars) == 0:
            census_vars = census_df
        else:
            census_vars = pd.concat([census_vars, census_df],
                                    ignore_index=True)
    census_vars['year'] = year

    return census_vars
