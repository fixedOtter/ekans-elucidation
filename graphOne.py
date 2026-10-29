import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
import graphOutliers as go

df = go.add_normalized_rank(go.load_all_outliers())
common = go.find_common_ssnamenr(df)
pool = df[(df.method == 'KNNSJ') & (df.comparison == 'color+amplitude')]

top1pct = go.top_active_trajectories(pool, common, top_pct=0.01, min_count=10)[('KNNSJ', 'color+amplitude')]

go.plot_trajectories(top1pct.iloc[[0]], f'color+amplitude')
plt.savefig('./tmp/one_object.png', dpi=110)

go.plot_trajectories(top1pct.iloc[:5], 'color+amplitude')
plt.savefig('./tmp/five_objects.png', dpi=110)

go.plot_trajectories(top1pct, f'color+amplitude')
plt.savefig('./tmp/top1pct.png', dpi=110)
# print('ok', len(top1pct))