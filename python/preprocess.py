import pandas as pd


def _prepare(input_parquet):
    df = pd.read_parquet(input_parquet)
    df = df.sort_values(['session', 'ts'])
    df['user_id'] = df['session'] + 1
    df['item_id'] = df['aid'] + 1
    return df[['user_id', 'item_id']]


def produce_txt(input_parquet, output_path):
    df = _prepare(input_parquet)
    df.to_csv(output_path, sep=' ', index=False, header=False)
    print(f"Written {len(df)} interactions to {output_path}")
    print(f"Users: {df['user_id'].max()}, Items: {df['item_id'].max()}")


def produce_parquet(input_parquet, output_path):
    df = _prepare(input_parquet)
    df.to_parquet(output_path, index=False)
    print(f"Written {len(df)} interactions to {output_path}")
    print(f"Users: {df['user_id'].max()}, Items: {df['item_id'].max()}")


produce_parquet('data/otto/train.parquet', 'data/otto.parquet')