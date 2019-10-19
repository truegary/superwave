# coding = utf-8

from stockutil.dataloadutil.loadfundutil import load_stockcode_in_catigery
from stockutil.dataloadutil.loadfundutil import load_multi_stock_datas
import matplotlib.pyplot as plt

main_type = "行业板块"
sub_type = "银行"


if __name__ == '__main__':
    code_list = load_stockcode_in_catigery(main_type, sub_type)

    dfcomp = load_multi_stock_datas(code_list, first_date="2007-01-01", last_date="2007-12-31")

    retscomp = dfcomp.pct_change()
    # Expected returns
    print(retscomp.mean())
    # Risk
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
