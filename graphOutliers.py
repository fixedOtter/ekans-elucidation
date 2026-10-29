#
# made by fixedotter 14.05.2026
#

import os
import re
import matplotlib.pyplot as plt
import pandas as pd

# maybe could move to env var
DATA_DIR = './data/outlier_score_files_ztf'

# holy fucking regex 
FILENAME_RE = re.compile(
  r'^ranking_(?P<method>DSSJ|KNNSJ)_(?P<comparison>[a-zA-Z+]+)_(?P<date>\d{2}_\d{2})\.parquet\.gzip$'
)

# DSSJ and KNNSJ each report a distance-style score and a connectivity-style
# score, just under different column names -- map both onto shared names so
# files from either method can be concatenated into one tidy dataframe.
COLUMN_MAP = {
  'DSSJ': {
    'Ranking: Mean Distance': 'rank_distance',
    'Mean Distances': 'distance',
    'Ranking: Num. Neighbors': 'rank_count',
    'Num. Neighbors': 'count',
  },
  'KNNSJ': {
    'Ranking: Mean Distance to k Neighbors': 'rank_distance',
    'Mean Distance to k Neighbors': 'distance',
    'Ranking: In-degree': 'rank_count',
    'In-degree Number': 'count',
  },
}


def discover_files(data_dir):
  """Walk year subdirectories and yield one entry per ranking file found."""
  for year in sorted(os.listdir(data_dir)):
    year_dir = os.path.join(data_dir, year)
    if not year.isdigit() or not os.path.isdir(year_dir):
      continue
    for filename in sorted(os.listdir(year_dir)):
      match = FILENAME_RE.match(filename)
      if not match:
        continue
      yield {
        'year': int(year),
        'method': match.group('method'),
        'comparison': match.group('comparison'),
        'path': os.path.join(year_dir, filename),
        }


def load_file(entry):
  """Load one ranking file and tag it with its year/method/comparison."""
  df = pd.read_parquet(entry['path'])
  df = df.rename(columns=COLUMN_MAP[entry['method']])
  df = df.rename(columns={'SSNAMENR': 'ssnamenr'})
  df['year'] = entry['year']
  df['method'] = entry['method']
  df['comparison'] = entry['comparison']
  return df[['year', 'method', 'comparison', 'ssnamenr', 'rank_distance', 'distance', 'rank_count', 'count']]


def load_all_outliers(data_dir=DATA_DIR):
  """Discover and load every ranking file under data_dir into one long-format dataframe."""
  frames = []
  for entry in discover_files(data_dir):
    try:
      frames.append(load_file(entry))
      print(f"Loaded {entry['path']} ({entry['year']}, {entry['method']}, {entry['comparison']})")
    except Exception as e:
      print(f"Error processing {entry['path']}: {e}")
      continue

  if not frames:
    raise RuntimeError(f"No ranking files found under {data_dir}")

  return pd.concat(frames, ignore_index=True)


def plot_top_outliers(all_outliers_df, method, comparison, year, top_n=25):
  """Bar chart of the top-N most outlying objects (lowest rank_distance) for one file."""
  subset = all_outliers_df[
    (all_outliers_df['method'] == method) &
    (all_outliers_df['comparison'] == comparison) &
    (all_outliers_df['year'] == year)
  ].nsmallest(top_n, 'rank_distance')

  plt.figure(figsize=(10, 6))
  plt.barh(subset['ssnamenr'].astype(str), subset['distance'])
  plt.gca().invert_yaxis()
  plt.title(f'Top {top_n} outliers: {method} {comparison} {year}')
  plt.xlabel('distance score')
  plt.ylabel('SSNAMENR')
  plt.tight_layout()
  plt.show()


def add_normalized_rank(all_outliers_df):
  """Add rank_distance normalized to [0, 1) by the number of objects scored
  in that object's (year, method, comparison) file, so ranks from files of
  different sizes (years have different catalog sizes) are comparable."""
  group_size = all_outliers_df.groupby(['year', 'method', 'comparison'])['ssnamenr'].transform('size')
  all_outliers_df['norm_rank_distance'] = all_outliers_df['rank_distance'] / group_size
  return all_outliers_df


def find_common_ssnamenr(all_outliers_df):
  """Asteroids present in every year. The object universe only depends on
  year (not method/comparison), so this set applies to every file-type."""
  sets_by_year = all_outliers_df.groupby('year')['ssnamenr'].apply(set)
  return set.intersection(*sets_by_year)


def top_active_trajectories(all_outliers_df, common_ssnamenr, top_pct=0.005, min_count=10):
  """For each (method, comparison), keep only asteroids common to every
  year, drop ones whose neighbor/in-degree count ever falls below
  min_count (their rank_distance is noisy from too little data and would
  otherwise dominate the 'most active' ranking with sparsity artifacts,
  not genuine outlier behavior), then rank the rest by the std deviation
  of their normalized rank_distance across years and return the top_pct
  most 'active' ones, as a {(method, comparison): ssnamenr x year
  dataframe} dict."""
  df = all_outliers_df[all_outliers_df['ssnamenr'].isin(common_ssnamenr)]
  trajectories = {}
  for group_key, group in df.groupby(['method', 'comparison']):
    reliable_ssnamenr = group.groupby('ssnamenr')['count'].min()
    reliable_ssnamenr = reliable_ssnamenr[reliable_ssnamenr >= min_count].index
    pivot = group[group['ssnamenr'].isin(reliable_ssnamenr)].pivot(
      index='ssnamenr', columns='year', values='norm_rank_distance')
    activity = pivot.std(axis=1)
    n_top = max(1, round(len(activity) * top_pct))
    top_ssnamenr = activity.nlargest(n_top).index
    trajectories[group_key] = pivot.loc[top_ssnamenr]
  return trajectories


def save_active_asteroids(trajectories, path='./data/most_active_asteroids.csv'):
  """Write the top-activity asteroid list (ssnamenr, method, comparison, per-year normalized score) to csv."""
  tagged = []
  for (method, comparison), pivot in trajectories.items():
    rows = pivot.reset_index()
    rows.insert(0, 'comparison', comparison)
    rows.insert(0, 'method', method)
    tagged.append(rows)
  pd.concat(tagged, ignore_index=True).to_csv(path, index=False)


def plot_trajectories(pivot, title, ax=None):
  """Line plot of every asteroid's normalized score across years, for one
  (method, comparison) pivot or any row-subset of it. Pass ax to draw into
  an existing subplot; omit it to get a standalone figure."""
  if ax is None:
    _, ax = plt.subplots(figsize=(6, 4))
  years = pivot.columns
  for _, row in pivot.iterrows():
    ax.plot(years, row.values, marker='o', alpha=0.6, linewidth=1)
  ax.set_title(title, fontsize=16)
  ax.set_xlabel('year', fontsize=12)
  ax.set_ylabel('normalized score', fontsize=12)
  ax.set_xticks(years)
  ax.grid(alpha=0.3)
  return ax


def plot_active_trajectories(trajectories):
  """Grid of line plots, one per (method, comparison), each showing every
  top-activity asteroid's normalized rank_distance across years."""
  ncols = 4
  nrows = -(-len(trajectories) // ncols)
  fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows), sharey=True)
  axes = axes.flatten()

  for ax, ((method, comparison), pivot) in zip(axes, trajectories.items()):
    plot_trajectories(pivot, f'{method}\n{comparison}\n(n={len(pivot)})', ax=ax)

  for ax in axes[len(trajectories):]:
    ax.axis('off')

  fig.suptitle('Top 0.5% most active outliers (highest std-dev of normalized score), common to all years')
  fig.tight_layout()
  plt.show()


if __name__ == "__main__":
  all_outliers_df = load_all_outliers()

  num_files = all_outliers_df[['year', 'method', 'comparison']].drop_duplicates().shape[0]
  print(f"Loaded {len(all_outliers_df)} rows across {num_files} files.")

  all_outliers_df = add_normalized_rank(all_outliers_df)
  common_ssnamenr = find_common_ssnamenr(all_outliers_df)
  print(f"{len(common_ssnamenr)} asteroids common to all years.")

  trajectories = top_active_trajectories(all_outliers_df, common_ssnamenr, top_pct=0.005, min_count=10)
  for (method, comparison), pivot in trajectories.items():
    print(f"{method} {comparison}: {len(pivot)} most active asteroids")

  save_active_asteroids(trajectories)
  plot_active_trajectories(trajectories)
