# coding = utf-8

from stockutil.dataloadutil.loadfundutil import load_multi_stock_datas
import matplotlib.pyplot as plt

dfcomp = load_multi_stock_datas(['000002', '600036', '002400', '600581', '600332'])

retscomp = dfcomp.pct_change()

print(retscomp.mean())

print(retscomp.std())

plt.scatter(retscomp.mean(), retscomp.std())

plt.xlabel('Expected returns')
plt.ylabel('Risk')

for label, x, y in zip(retscomp.columns, retscomp.mean(), retscomp.std()):
    plt.annotate(label, xy=(x, y), xytext=(20, -20),
                 textcoords='offset points', ha='right', va='bottom',
                 bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
                 arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
plt.show()
