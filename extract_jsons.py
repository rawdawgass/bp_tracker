import json, os, time
import platform, subprocess
import pandas as pd
from pandas.io.json import json_normalize
from config import bps_lst

bp_jsons_lst = [os.path.join(root,f) for root,dirs,files in os.walk('bp_jsons') for f in files if f.endswith('.json')]


def execute_bash(command):
    is32bit = (platform.architecture()[0] == '32bit')
    system32 = os.path.join(os.environ['SystemRoot'],
                            'SysNative' if is32bit else 'System32')
    bash = os.path.join(system32, 'bash.exe')
    subprocess.call('"%s" -c "{}"'.format(command) % bash)

#get most outter
def extract_jsons():
    for bp in bps_lst:
        call = "cleos -u http://api.proxy1a.sheos.org get actions {} 0,0, 10000 -j > ".format(bp) + "bp_jsons/{}.json".format(bp)

        #without full list
        #call = "cleos -u http://api.proxy1a.sheos.org get actions {} -j > ".format(bp) + "bp_jsons/{}.json".format(bp)

        print ('extracting {}...'.format(bp))
        execute_bash(call)
        #time.sleep(60)
        print ('[DONE]'.format(bp))


    outter_df = pd.DataFrame()
    for bp_json in bp_jsons_lst:

        with open(bp_json) as f:
            data  = json.load(f)
        data_df = json_normalize(data, [['actions']],errors='ignore')
        data_df = data_df.rename(columns={'global_action_seq':'global_sequence'})
        data_df['bp_id'] = os.path.basename(bp_json).replace('.json','')

        #put block_id right here
        outter_df = outter_df.append(data_df, sort=False)
    #outter_df.to_csv('outter_df.csv')
    return outter_df

#receipt and act!
def compile_actions():
    print ('Compiling...')

    #real
    outter_df = extract_jsons()
    #for testing
    #outter_df = pd.read_csv('outter_df.csv')

    inline_df = pd.DataFrame()
    for x in outter_df['action_trace']:
        for d in x['inline_traces']:
            df = pd.DataFrame(d['receipt'])
            df['type'] = d['act']['name']
            df['trx_id'] = d['trx_id']

            #quantity stuff
            for quant_col in ['quantity', 'memo', 'to', 'from']:
                try:
                    df[quant_col] = d['act']['data'][quant_col]
                except:
                    pass
            inline_df = inline_df.append(df, sort=True)

    #will make this shit cleaner later
    action_df = pd.DataFrame()
    for d in outter_df['action_trace']:
        #for d in x['inline_traces']:
            df = pd.DataFrame(d['receipt'])
            df['type'] = d['act']['name']
            df['trx_id'] = d['trx_id']

            #quantity stuff
            for quant_col in ['quantity', 'memo', 'to', 'from']:
                try:
                    df[quant_col] = d['act']['data'][quant_col]
                except:
                    pass
            action_df = action_df.append(df, sort=False)

    compile_df = pd.concat([inline_df, action_df], sort=False)

    compile_df = compile_df.merge(outter_df, on='global_sequence', how='left')

    #compile_df['bp_id'] = compile_df['auth_sequence'].apply(lambda row: row[0])

    compile_df['seq'] = compile_df['auth_sequence'].apply(lambda row: row[1])


    def keep(row):
        if pd.isnull(row['to']) and pd.isnull(row['from']):
            return 'y'

        if row['bp_id'] == row['to'] or row['bp_id'] == row['from']:
            return 'y'


    rev_cols = ['global_sequence', 'block_time', 'bp_id', 'seq', 'type', 'from', 'to', 'quantity', 'memo', 'trx_id', 'block_num']
    compile_df = compile_df[rev_cols].sort_values(by=['global_sequence', 'block_time'])

    for x in ['block_time', 'block_num', 'bp_id']:
        compile_df[x] = compile_df[x].fillna(method='ffill')

    compile_df = compile_df.drop_duplicates(subset=['quantity', 'trx_id', 'block_num'])

    compile_df['keep'] = compile_df.apply(keep, axis=1)
    compile_df = compile_df[compile_df['keep']=='y'].drop(columns='keep')


    #clean quantity
    def quantity_clean(row):
        if pd.notnull(row['quantity']):
            quantity = float(str(row['quantity']).replace(' EOS',''))
            #return quantity
            if row['bp_id'] == row['from']:
                return -quantity

            elif row['bp_id'] == row['to']:
                return quantity


    compile_df['quantity'] = compile_df.apply(quantity_clean, axis=1)


    compile_df.to_csv('compile_df.csv', index=False)
    #compile_df.to_excel('compile_df.xlsx', index=False)
    print ('Compiled!')
