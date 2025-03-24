import pandas as pd
from datetime import datetime

INPUT_CSV_FILENAME = 'iusd_transfers.csv'
OUTPUT_CSV_FILENAME = 'processed_iusd_transfers.csv'
VELO_VOLATILE_CSV_FILENAME = 'velo_volatile_mint_burn_events.csv'

CUTOFF_DATE = '03-17-2025'

LP_CONTRACT_ADDRESS_LIST = [# '0x2815bF2bDd198E6d09B9F02Ef6D62281b2FaAdB7', 
                            '0x0f53E9d4147c2073cc64a70FFc0fec9606E2EEb7', 
                            '0xEC1D7b7058dF61ef9401DB56DbF195388b77EABa', 
                            '0xA7F102e1CeC3883C2e7Ae3cD24126f836675EfEB']

# Makes our nice mm-dd-yyyy day column
def make_day_column(df):
    # Create 'day' column while preserving the original timestamp
    df['day'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x).strftime('%m-%d-%Y'))
    return df

# # returns a dataframe with only activity before the cutoff day
def get_cutoff_day_df(df):
    df = df.loc[df['day'] <= CUTOFF_DATE]

    return df

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

    df['amount'] = df['amount0'] + df['amount1']

    df = df[['timestamp', 'tx_hash', 'address', 'amount0', 'amount1', 'amount', 'event_type']]

    df = df.sort_values('timestamp', ascending=True)

    df.loc[df['event_type'] == 'burn', 'amount'] = df['amount'] * -1
    # Group by address and calculate cumulative sum within each group
    df['balance'] = df.groupby('address')['amount'].cumsum()

    df = make_day_column(df)
    df = get_cutoff_day_df(df)

    return df

# Our runner function
def run_all():
    df = pd.read_csv(INPUT_CSV_FILENAME)
    df = df.drop_duplicates(subset=['block_number','timestamp','tx_hash','from_address','to_address','amount'])
    df[['amount','timestamp']] = df[['amount','timestamp']].astype(float)

    df = make_day_column(df)
    df = get_cutoff_day_df(df)

    df = calculate_running_balances(df)

    # Get users' last balances
    last_user_balance_df = get_users_last_balances(df)
    
    # Get users' LP positions
    pool_balance_df = get_user_velo_volatile_lp_balance()

    # Save all results to CSV files
    df.to_csv(OUTPUT_CSV_FILENAME, index=False)
    last_user_balance_df.to_csv('last_user_balances.csv', index=False)
    pool_balance_df.to_csv('pool_balances.csv', index=False)
    
    return df, last_user_balance_df, pool_balance_df

# Run everything and print sample results
# df, last_user_balances, pool_balance_df = run_all()
# print("Transaction data sample:")
# print(df.head())
# print("\nTop 10 user balances:")
# print(last_user_balances.head(10))
# print("\nTop 10 LP positions:")
# print(pool_balance_df.head(10))

df = get_user_velo_volatile_lp_balance()
print(df)
df.to_csv('test_test.csv', index=False)