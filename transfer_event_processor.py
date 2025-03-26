import pandas as pd
from datetime import datetime
from web3 import Web3
import time as tt

INPUT_CSV_FILENAME = 'iusd_transfers.csv'
OUTPUT_CSV_FILENAME = 'processed_iusd_transfers.csv'
VELO_VOLATILE_CSV_FILENAME = 'velo_volatile_mint_burn_events.csv'

CUTOFF_DATE = '03-17-2025'

LP_CONTRACT_ADDRESS_LIST = [# '0x2815bF2bDd198E6d09B9F02Ef6D62281b2FaAdB7', 
                            '0x0f53E9d4147c2073cc64a70FFc0fec9606E2EEb7', 
                            '0xEC1D7b7058dF61ef9401DB56DbF195388b77EABa', 
                            '0xA7F102e1CeC3883C2e7Ae3cD24126f836675EfEB'
                            ]

LP_CSV_NAME_LIST = [
                    'velo_volatile_mint_burn_events.csv',
                    'velo_cl_mint_burn_events.csv',
                    'kim_cl_mint_burn_events.csv'
                    ]

w3 = Web3(Web3.HTTPProvider('https://mainnet.mode.network'))

WAIT_TIME = 0.5

# Makes our nice mm-dd-yyyy day column
def make_day_column(df):
    # Create 'day' column while preserving the original timestamp
    df['day'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x).strftime('%m-%d-%Y'))
    return df

# # returns a dataframe with only activity before the cutoff day
def get_cutoff_day_df(df):
    df = df.loc[df['day'] <= CUTOFF_DATE]

    return df

# # will return a dataframe that only contains token transfers that occur on the same tx hashes as our adding or removing liquidity from lps
def match_transaction_hashes_df(transfer_df, lp_df):

    unique_lp_tx_hash_list = lp_df['tx_hash'].unique()

    shared_tx_hash_df = transfer_df.loc[transfer_df['tx_hash'].isin(unique_lp_tx_hash_list)]

    return shared_tx_hash_df

# # will filter our transactions down to just those that go to or are from the lp_address
def filter_to_lp_transfers(df, lp_address):

    lp_address = lp_address.lower()

    df_list = []

    temp_df = df.loc[df['from_address'] == lp_address]
    df_list.append(temp_df)
    temp_df = df.loc[df['to_address'] == lp_address]
    df_list.append(temp_df)

    df = pd.concat(df_list)

    return df

# # gets the most recent user balance
def get_last_user_balance(df):

    unique_user_list = df['address'].unique()

    df_list = []

    # # filters dataframe down to subset of just each user, then down to each user's last transaction, then adds the 1 row df to df_list to later be combined with other users last event
    for unique_user in unique_user_list:
        temp_df = df.loc[df['address'] == unique_user]
        last_timestamp = temp_df['timestamp'].max()

        temp_df = temp_df.loc[temp_df['timestamp'] == last_timestamp]
        max_balance = temp_df['balance'].astype(float).max()
        
        temp_df = temp_df.loc[temp_df['balance'].astype(float) == max_balance]

        df_list.append(temp_df)
    
    try:
        df = pd.concat(df_list)
    except:
        print(df)
        print(unique_user_list)

    df['last_balance'] = df['balance']

    df = df[['address', 'last_balance']]
    df['last_balance'] /= 1e18

    return df

# # will estimate the balances of users
def get_rolling_balance(df, lp_address):
    
    df_list = []

    if len(lp_address) > 0:
        lp_address = lp_address.lower()

        mint_df = df.loc[df['to_address'] == lp_address]
        mint_df['address'] = mint_df['to_address']

        burn_df = df.loc[df['from_address'] == lp_address]
        burn_df['address'] = burn_df['from_address']

        burn_df['amount'] = burn_df['amount'] * -1

        df_list.append(mint_df)
        df_list.append(burn_df)
    
    else:
        from_df = df.copy()
        from_df['address'] = df['from_address']
        from_df['amount'] = from_df['amount'] * -1

        df['address'] = df['to_address']

        df_list.append(from_df)
        df_list.append(df)


    df = pd.concat(df_list)
    df = df.sort_values(by='timestamp', ascending=True)

    df['balance'] = df.groupby('address')['amount'].cumsum()

    return df

# # Returns True if it is a contract. Returns False if it is an EOA
def is_contract(address, w3):
    # Get the bytecode at the address
    code = w3.eth.get_code(address)
    
    # If the bytecode is empty (just '0x'), it's an EOA
    # If it contains code, it's a contract
    return code != '0x' and code != b'0x' and code != b'' and code != ''

# # will label our df addresses on whether they are contracts or not
def label_contracts(df, w3):

    unique_address_list = df['address'].unique()
    
    wallet_type_list = []

    try:
        existing_label_df = pd.read_csv('existing_labels.csv')
    except:
        existing_label_df = pd.DataFrame()

    i = 0

    while i < len(unique_address_list):
        wallet_type = ''
        is_address_contract = False
        unique_address_og = unique_address_list[i]
        unique_address = w3.to_checksum_address(unique_address_og)

        # # logic so we don't have to see what wallet_type someone is if we have previously labeled it
        if len(existing_label_df) > 0:
            temp_df = existing_label_df.loc[existing_label_df['address'] == unique_address_og]

            if len(temp_df) > 0:
                wallet_type = temp_df['wallet_type'].tolist()[0]
        
        if len(wallet_type) < 1:
            is_address_contract = is_contract(unique_address, w3)
            tt.sleep(WAIT_TIME)
        # # end already_exists checking logic

            if is_address_contract == True:
                wallet_type = 'contract'
            
            else:
                wallet_type = 'eoa'
        
        wallet_type_list.append(wallet_type)

        i += 1
        print('Addresses Checked: ', i, '/',len(unique_address_list))

    df['wallet_type'] = wallet_type_list

    df.to_csv('existing_labels.csv', index=False)



    return df


# # will find our how much each user has sent to each contract (lps, stability pool, etc.)
def get_user_share_of_contract_balance(original_transfer_df, labeled_df):

    # # filters down to just wallets that are contracts
    contract_df = labeled_df.loc[labeled_df['wallet_type'] == 'contract']

    # # puts all the contract addresses into a list
    contract_address_list = contract_df['address'].unique()

    df_list = []

    # # for every contract address
    for contract_address in contract_address_list:
        
        temp_df_list = []
        temp_df = original_transfer_df.copy()

        # # widthraws
        temp_df = temp_df.loc[temp_df['from_address'] == contract_address]
        temp_df['amount'] = temp_df['amount'] * -1
        temp_df['address'] = temp_df['from_address']
        temp_df_list.append(temp_df)

        # # deposits
        temp_df = original_transfer_df.copy()
        temp_df = temp_df.loc[temp_df['to_address'] == contract_address]
        temp_df['address'] = temp_df['to_address']

        temp_df = pd.concat(temp_df_list)
        temp_df = temp_df.sort_values(by='timestamp', ascending=True)

        temp_df['balance'] = temp_df.groupby('address')['amount'].cumsum()

        if len(temp_df) > 0:
            temp_df = get_last_user_balance(temp_df)
            df_list.append(temp_df)
        else:
            print('Empty Balance address: ', contract_address)
    
    print(df_list)
    df = pd.concat(df_list)

    return df

# # will find the percentage of a pool someone had at time of snapshot
# # then finds their iUSD equivalent of the pool at the time of the snapshot
def get_share_of_lp(lp_df):


    # # filters out any negative balances
    lp_df = lp_df.loc[lp_df['last_balance'] > 0]

    # # just sums the non-zero balances together to estimate how much iUSD there was
    pool_total_token = lp_df['last_balance'].sum()

    lp_df['percentage_of_lp'] = lp_df['last_balance'] / pool_total_token
    lp_df['token_equivalent_of_lp'] = lp_df['percentage_of_lp'] * pool_total_token

    print('Total Token in Pool: ', pool_total_token)

    return lp_df

# Track running balances for all addresses
def calculate_running_balances(df):
    # Sort by timestamp to ensure chronological processing
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # Get all unique addresses from both from_address and to_address
    from_addresses = set(df['from_address'])
    to_addresses = set(df['to_address'])
    all_addresses = from_addresses.union(to_addresses)
    
    # Initialize balances for all addresses
    balances = {addr: 0 for addr in all_addresses}
    
    # Create columns to store running balances
    df['from_balance_after'] = 0.0
    df['to_balance_after'] = 0.0
    
    # Process each transaction
    for idx, row in df.iterrows():
        from_addr = row['from_address']
        to_addr = row['to_address']
        amount = row['amount']
        
        # Deduct from sender
        balances[from_addr] -= amount
        
        # Add to receiver
        balances[to_addr] += amount
        
        # Store the updated balances
        df.at[idx, 'from_balance_after'] = balances[from_addr]
        df.at[idx, 'to_balance_after'] = balances[to_addr]
    
    return df


def get_users_last_balances(df):
    """
    Calculate each user's most recent balance based on their latest transaction.
    
    Args:
        df: DataFrame with transaction data and running balances
        
    Returns:
        DataFrame with each address and their latest balance
    """
    # Ensure data is sorted by timestamp
    df = df.sort_values('timestamp')
    
    # Get all unique addresses
    from_addresses = set(df['from_address'])
    to_addresses = set(df['to_address'])
    all_addresses = from_addresses.union(to_addresses)
    
    # Initialize dictionary to store last balances
    last_balances = {}
    
    # For each address, find their latest transaction and balance
    for address in all_addresses:
        # Get rows where address is sender
        mask_from = df['from_address'] == address
        # Get rows where address is receiver 
        mask_to = df['to_address'] == address
        
        # Find last transaction where this address appears
        last_from_tx = df[mask_from].iloc[-1] if any(mask_from) else None
        last_to_tx = df[mask_to].iloc[-1] if any(mask_to) else None
        
        # Determine which transaction is more recent
        if last_from_tx is not None and last_to_tx is not None:
            if last_from_tx['timestamp'] > last_to_tx['timestamp']:
                last_balances[address] = last_from_tx['from_balance_after']
            else:
                last_balances[address] = last_to_tx['to_balance_after']
        elif last_from_tx is not None:
            last_balances[address] = last_from_tx['from_balance_after']
        elif last_to_tx is not None:
            last_balances[address] = last_to_tx['to_balance_after']
    
    # Convert to DataFrame
    last_balance_df = pd.DataFrame({
        'address': list(last_balances.keys()),
        'last_balance': list(last_balances.values())
    })
    
    # Sort by balance descending
    last_balance_df = last_balance_df.sort_values('last_balance', ascending=False).reset_index(drop=True)
    
    last_balance_df['last_balance'] = last_balance_df['last_balance'] / 1e18

    return last_balance_df

# # our regular volatile swap pool processing
def get_user_velo_volatile_lp_balance():
    df = pd.read_csv(VELO_VOLATILE_CSV_FILENAME)

    df['address'] = df['to_address']
    
    df[['timestamp', 'amount0', 'amount1']] = df[['timestamp','amount0', 'amount1']].astype(float)

    df['amount0'] /= 1e18
    df['amount1'] /= 1e6

    # # would get total inputs and outputs for both tokens in pair
    # df['amount'] = df['amount0'] + df['amount1']
    df['amount'] = df['amount0']

    df = df[['timestamp', 'tx_hash', 'address', 'amount0', 'amount1', 'amount', 'event_type']]

    df = df.sort_values('timestamp', ascending=True)

    df.loc[df['event_type'] == 'burn', 'amount'] = df['amount'] * -1
    # Group by address and calculate cumulative sum within each group
    df['balance'] = df.groupby('address')['amount'].cumsum()

    df = make_day_column(df)
    df = get_cutoff_day_df(df)
    df = get_last_user_balance(df)

    return df

# Our runner function
def run_all():
    df = pd.read_csv(INPUT_CSV_FILENAME)
    df = df.drop_duplicates(subset=['block_number','timestamp','tx_hash','from_address','to_address','amount'])
    df[['amount','timestamp']] = df[['amount','timestamp']].astype(float)

    df = make_day_column(df)
    og_df = df.copy()
    # df = get_cutoff_day_df(df)
    
    # i = 2

    # while i < len(LP_CSV_NAME_LIST):

    #     lp_df = pd.read_csv(LP_CSV_NAME_LIST[i])
    #     lp_address = LP_CONTRACT_ADDRESS_LIST[i]

    #     lp_df = match_transaction_hashes_df(df, lp_df)

    #     lp_df = filter_to_lp_transfers(lp_df, lp_address)

    #     lp_df = get_rolling_balance(lp_df, lp_address)

    #     lp_df = get_last_user_balance(lp_df)

    #     i += 1
    
    df = get_rolling_balance(df, '')
    df = get_last_user_balance(df)
    df = label_contracts(df, w3)
    df = df.loc[df['last_balance'] > 0]
    df = df.sort_values(by='last_balance', ascending=False)
    
    # # work on this *****
    lp_df = get_user_share_of_contract_balance(og_df, df)

    return lp_df

# Run everything and print sample results
df = run_all()
# df = df.loc[df['address'] == '0x0f53E9d4147c2073cc64a70FFc0fec9606E2EEb7'.lower()]
print("Transaction data sample:")
print(df.head())
print('Long: ', len(df))
df.to_csv('test_test.csv', index=False)