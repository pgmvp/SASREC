import argparse
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Preprocess Otto parquet data for SASRec.')
    parser.add_argument('input', help='Path to input .parquet file (e.g. data/otto/train.parquet)')
    parser.add_argument('output', help='Path to output file (e.g. data/otto.txt or data/otto.parquet)')
    parser.add_argument(
        '--format', choices=['txt', 'parquet'], default=None,
        help='Output format. Inferred from output file extension when omitted.',
    )
    args = parser.parse_args()

    fmt = args.format
    if fmt is None:
        fmt = 'parquet' if args.output.endswith('.parquet') else 'txt'

    if fmt == 'parquet':
        produce_parquet(args.input, args.output)
    else:
        produce_txt(args.input, args.output)