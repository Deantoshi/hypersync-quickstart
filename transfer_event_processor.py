import pandas as pd
from datetime import datetime

INPUT_CSV_FILENAME = 'iusd_transfers.csv'
OUTPUT_CSV_FILENAME = 'processed_iusd_transfers.csv'
LP_CONTRACT_ADDRESS_LIST = ['0x2815bF2bDd198E6d09B9F02Ef6D62281b2FaAdB7', 
                            '0x0f53E9d4147c2073cc64a70FFc0fec9606E2EEb7', 
                            '0xEC1D7b7058dF61ef9401DB56DbF195388b77EABa', 
                            '0xA7F102e1CeC3883C2e7Ae3cD24126f836675EfEB']

# Makes our nice mm-dd-yyyy day column
def make_day_column(df):
    # Create 'day' column while preserving the original timestamp
    df['day'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x).strftime('%m-%d-%Y'))
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

def get_user_lp_balance(df):
    """
    Track users' LP positions by monitoring transactions with known liquidity pool addresses.
    
    When a user sends iUSD to the pool (adding liquidity):
        - Their pool balance increases
    When a user receives iUSD from the pool (removing liquidity):
        - Their pool balance decreases
        
    Args:
        df: DataFrame with transaction data
        
    Returns:
        DataFrame with each address and their current pool balance
    """
    lp_contract_address_list = [x.lower() for x in LP_CONTRACT_ADDRESS_LIST]
    
    # Convert list to set for faster lookups
    lp_addresses = set(lp_contract_address_list)
    
    # Filter transactions involving LP addresses
    lp_txs = df[(df['from_address'].isin(lp_addresses)) | (df['to_address'].isin(lp_addresses))].copy()
    
    # Sort by timestamp to ensure chronological processing
    lp_txs = lp_txs.sort_values('timestamp').reset_index(drop=True)
    
    # Initialize user pool balances dictionary
    user_pool_balances = {}
    
    # Process each LP transaction
    for _, tx in lp_txs.iterrows():
        from_addr = tx['from_address']
        to_addr = tx['to_address']
        amount = float(tx['amount'])  # Ensure amount is float
        
        # Case 1: User sends to pool (adding liquidity)
        if to_addr in lp_addresses and from_addr not in lp_addresses:
            user = from_addr
            # Initialize user if not present
            if user not in user_pool_balances:
                user_pool_balances[user] = 0
            # Add to user's pool balance when sending to pool
            user_pool_balances[user] += amount
        
        # Case 2: User receives from pool (removing liquidity)
        elif from_addr in lp_addresses and to_addr not in lp_addresses:
            user = to_addr
            # Initialize user if not present
            if user not in user_pool_balances:
                user_pool_balances[user] = 0
            # Subtract from user's pool balance when receiving from pool
            user_pool_balances[user] -= amount
    
    # Create a DataFrame from the user pool balances
    pool_balance_df = pd.DataFrame({
        'address': list(user_pool_balances.keys()),
        'pool_balance': list(user_pool_balances.values())
    })
    
    # Convert to human-readable format (assuming amounts are in wei/10^18)
    pool_balance_df['pool_balance'] = pool_balance_df['pool_balance'] / 1e18
    
    # Sort by pool balance descending
    pool_balance_df = pool_balance_df.sort_values('pool_balance', ascending=False).reset_index(drop=True)
    
    return pool_balance_df

# Our runner function
def run_all():
    df = pd.read_csv(INPUT_CSV_FILENAME)
    df = df.drop_duplicates(subset=['block_number','timestamp','tx_hash','from_address','to_address','amount'])
    df[['amount','timestamp']] = df[['amount','timestamp']].astype(float)

    df = make_day_column(df)
    df = df.loc[df['day'] <= '03-17-2025']
    df = calculate_running_balances(df)

    # Get users' last balances
    last_user_balance_df = get_users_last_balances(df)
    
    # Get users' LP positions
    pool_balance_df = get_user_lp_balance(df)

    # Save all results to CSV files
    df.to_csv(OUTPUT_CSV_FILENAME, index=False)
    last_user_balance_df.to_csv('last_user_balances.csv', index=False)
    pool_balance_df.to_csv('pool_balances.csv', index=False)
    
    return df, last_user_balance_df, pool_balance_df

# Run everything and print sample results
df, last_user_balances, pool_balance_df = run_all()
print("Transaction data sample:")
print(df.head())
print("\nTop 10 user balances:")
print(last_user_balances.head(10))
print("\nTop 10 LP positions:")
print(pool_balance_df.head(10))