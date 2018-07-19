import pandas as pd
from pandas.io.json import json_normalize
from airtable import Airtable
import json
import config

#airtable creds'
bp_actions_tbl = Airtable(config.base_key, config.bp_actions_key, api_key=config.api_key)
bp_key_tbl = Airtable(config.base_key, config.bp_key, api_key=config.api_key)

#print (bp_key_tbl.match('Name','eosfishrocks').get('id'))

def get_airtable():
    bp_actions_df = json_normalize(bp_actions_tbl.get_all(view='Grid view'))
    #rename cols remove airtables addition
    bp_actions_df.columns = [x.replace('fields.','') for x in bp_actions_df.columns]
    #bp_actions_df.to_csv('at_df.csv', index=False)
    return bp_actions_df

#get_airtable()


def add_uniques():
    bp_actions_df = get_airtable()
    compile_df = pd.read_csv('compile_df.csv')

    #get new rows in the extract that are not already in the airtable
    common = bp_actions_df.merge(compile_df,on=['global_sequence'])
    new_actions_df = compile_df[(~compile_df.global_sequence.isin(common.global_sequence))]
    new_actions_df = new_actions_df.fillna('')
    new_rows = len(new_actions_df)
    new_actions_dict = new_actions_df.to_dict(orient='records')

    for action in new_actions_dict:
        action2 = {k: v for k, v in action.items() if v}
        bp_actions_tbl.insert(action2, typecast=True)
    print ('{} rows added'.format(new_rows))
