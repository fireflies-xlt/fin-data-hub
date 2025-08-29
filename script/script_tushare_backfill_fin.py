import logging
import sys
import pandas as pd
import argparse
import time
from fin_data_hub.foundation.mysql.mysql_engine import mysql_engine, table_exists_and_not_empty
from fin_data_hub.data.tushare.constants import (
    STOCK_BASIC_TABLE,
    TRADE_CALENDAR_TABLE,
    INCOME_TABLE,
    BALANCESHEET_TABLE
)
from fin_data_hub.foundation.mysql.mysql_engine import (
    mysql_engine, 
)
from fin_data_hub.data.tushare.tushare_data import get_tushare_client

from fin_data_hub.foundation.utils.date_utils import (
    get_stock_start_date, 
    current_date_ymd, 
    get_all_quarter_end
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def get_stock_list() -> pd.DataFrame:
    """
    获取股票列表
    """
    query = f"SELECT * FROM {STOCK_BASIC_TABLE}"
    df = pd.read_sql(query, mysql_engine())
    return df


def backfill_income_data():
    """
    补全财务数据
    
    ts_code	str	Y	TS代码
    ann_date	str	Y	公告日期
    f_ann_date	str	Y	实际公告日期
    end_date	str	Y	报告期
    report_type	str	Y	报告类型 见底部表
    comp_type	str	Y	公司类型(1一般工商业2银行3保险4证券)
    end_type	str	Y	报告期类型
    basic_eps	float	Y	基本每股收益
    diluted_eps	float	Y	稀释每股收益
    total_revenue	float	Y	营业总收入
    revenue	float	Y	营业收入
    int_income	float	Y	利息收入
    prem_earned	float	Y	已赚保费
    comm_income	float	Y	手续费及佣金收入
    n_commis_income	float	Y	手续费及佣金净收入
    n_oth_income	float	Y	其他经营净收益
    n_oth_b_income	float	Y	加:其他业务净收益
    prem_income	float	Y	保险业务收入
    out_prem	float	Y	减:分出保费
    une_prem_reser	float	Y	提取未到期责任准备金
    reins_income	float	Y	其中:分保费收入
    n_sec_tb_income	float	Y	代理买卖证券业务净收入
    n_sec_uw_income	float	Y	证券承销业务净收入
    n_asset_mg_income	float	Y	受托客户资产管理业务净收入
    oth_b_income	float	Y	其他业务收入
    fv_value_chg_gain	float	Y	加:公允价值变动净收益
    invest_income	float	Y	加:投资净收益
    ass_invest_income	float	Y	其中:对联营企业和合营企业的投资收益
    forex_gain	float	Y	加:汇兑净收益
    total_cogs	float	Y	营业总成本
    oper_cost	float	Y	减:营业成本
    int_exp	float	Y	减:利息支出
    comm_exp	float	Y	减:手续费及佣金支出
    biz_tax_surchg	float	Y	减:营业税金及附加
    sell_exp	float	Y	减:销售费用
    admin_exp	float	Y	减:管理费用
    fin_exp	float	Y	减:财务费用
    assets_impair_loss	float	Y	减:资产减值损失
    prem_refund	float	Y	退保金
    compens_payout	float	Y	赔付总支出
    reser_insur_liab	float	Y	提取保险责任准备金
    div_payt	float	Y	保户红利支出
    reins_exp	float	Y	分保费用
    oper_exp	float	Y	营业支出
    compens_payout_refu	float	Y	减:摊回赔付支出
    insur_reser_refu	float	Y	减:摊回保险责任准备金
    reins_cost_refund	float	Y	减:摊回分保费用
    other_bus_cost	float	Y	其他业务成本
    operate_profit	float	Y	营业利润
    non_oper_income	float	Y	加:营业外收入
    non_oper_exp	float	Y	减:营业外支出
    nca_disploss	float	Y	其中:减:非流动资产处置净损失
    total_profit	float	Y	利润总额
    income_tax	float	Y	所得税费用
    n_income	float	Y	净利润(含少数股东损益)
    n_income_attr_p	float	Y	净利润(不含少数股东损益)
    minority_gain	float	Y	少数股东损益
    oth_compr_income	float	Y	其他综合收益
    t_compr_income	float	Y	综合收益总额
    compr_inc_attr_p	float	Y	归属于母公司(或股东)的综合收益总额
    compr_inc_attr_m_s	float	Y	归属于少数股东的综合收益总额
    ebit	float	Y	息税前利润
    ebitda	float	Y	息税折旧摊销前利润
    insurance_exp	float	Y	保险业务支出
    undist_profit	float	Y	年初未分配利润
    distable_profit	float	Y	可分配利润
    rd_exp	float	Y	研发费用
    fin_exp_int_exp	float	Y	财务费用:利息费用
    fin_exp_int_inc	float	Y	财务费用:利息收入
    transfer_surplus_rese	float	Y	盈余公积转入
    transfer_housing_imprest	float	Y	住房周转金转入
    transfer_oth	float	Y	其他转入
    adj_lossgain	float	Y	调整以前年度损益
    withdra_legal_surplus	float	Y	提取法定盈余公积
    withdra_legal_pubfund	float	Y	提取法定公益金
    withdra_biz_devfund	float	Y	提取企业发展基金
    withdra_rese_fund	float	Y	提取储备基金
    withdra_oth_ersu	float	Y	提取任意盈余公积金
    workers_welfare	float	Y	职工奖金福利
    distr_profit_shrhder	float	Y	可供股东分配的利润
    prfshare_payable_dvd	float	Y	应付优先股股利
    comshare_payable_dvd	float	Y	应付普通股股利
    capit_comstock_div	float	Y	转作股本的普通股股利
    net_after_nr_lp_correct	float	N	扣除非经常性损益后的净利润（更正前）
    credit_impa_loss	float	N	信用减值损失
    net_expo_hedging_benefits	float	N	净敞口套期收益
    oth_impair_loss_assets	float	N	其他资产减值损失
    total_opcost	float	N	营业总成本（二）
    amodcost_fin_assets	float	N	以摊余成本计量的金融资产终止确认收益
    oth_income	float	N	其他收益
    asset_disp_income	float	N	资产处置收益
    continued_net_profit	float	N	持续经营净利润
    end_net_profit	float	N	终止经营净利润
    update_flag	str	Y	更新标识

    """
    backfill_income_data_1()


def backfill_balancesheet_data():
    """
    补全资产负债表

    ts_code	str	Y	TS股票代码
    ann_date	str	Y	公告日期
    f_ann_date	str	Y	实际公告日期
    end_date	str	Y	报告期
    report_type	str	Y	报表类型
    comp_type	str	Y	公司类型(1一般工商业2银行3保险4证券)
    end_type	str	Y	报告期类型
    total_share	float	Y	期末总股本
    cap_rese	float	Y	资本公积金
    undistr_porfit	float	Y	未分配利润
    surplus_rese	float	Y	盈余公积金
    special_rese	float	Y	专项储备
    money_cap	float	Y	货币资金
    trad_asset	float	Y	交易性金融资产
    notes_receiv	float	Y	应收票据
    accounts_receiv	float	Y	应收账款
    oth_receiv	float	Y	其他应收款
    prepayment	float	Y	预付款项
    div_receiv	float	Y	应收股利
    int_receiv	float	Y	应收利息
    inventories	float	Y	存货
    amor_exp	float	Y	待摊费用
    nca_within_1y	float	Y	一年内到期的非流动资产
    sett_rsrv	float	Y	结算备付金
    loanto_oth_bank_fi	float	Y	拆出资金
    premium_receiv	float	Y	应收保费
    reinsur_receiv	float	Y	应收分保账款
    reinsur_res_receiv	float	Y	应收分保合同准备金
    pur_resale_fa	float	Y	买入返售金融资产
    oth_cur_assets	float	Y	其他流动资产
    total_cur_assets	float	Y	流动资产合计
    fa_avail_for_sale	float	Y	可供出售金融资产
    htm_invest	float	Y	持有至到期投资
    lt_eqt_invest	float	Y	长期股权投资
    invest_real_estate	float	Y	投资性房地产
    time_deposits	float	Y	定期存款
    oth_assets	float	Y	其他资产
    lt_rec	float	Y	长期应收款
    fix_assets	float	Y	固定资产
    cip	float	Y	在建工程
    const_materials	float	Y	工程物资
    fixed_assets_disp	float	Y	固定资产清理
    produc_bio_assets	float	Y	生产性生物资产
    oil_and_gas_assets	float	Y	油气资产
    intan_assets	float	Y	无形资产
    r_and_d	float	Y	研发支出
    goodwill	float	Y	商誉
    lt_amor_exp	float	Y	长期待摊费用
    defer_tax_assets	float	Y	递延所得税资产
    decr_in_disbur	float	Y	发放贷款及垫款
    oth_nca	float	Y	其他非流动资产
    total_nca	float	Y	非流动资产合计
    cash_reser_cb	float	Y	现金及存放中央银行款项
    depos_in_oth_bfi	float	Y	存放同业和其它金融机构款项
    prec_metals	float	Y	贵金属
    deriv_assets	float	Y	衍生金融资产
    rr_reins_une_prem	float	Y	应收分保未到期责任准备金
    rr_reins_outstd_cla	float	Y	应收分保未决赔款准备金
    rr_reins_lins_liab	float	Y	应收分保寿险责任准备金
    rr_reins_lthins_liab	float	Y	应收分保长期健康险责任准备金
    refund_depos	float	Y	存出保证金
    ph_pledge_loans	float	Y	保户质押贷款
    refund_cap_depos	float	Y	存出资本保证金
    indep_acct_assets	float	Y	独立账户资产
    client_depos	float	Y	其中：客户资金存款
    client_prov	float	Y	其中：客户备付金
    transac_seat_fee	float	Y	其中:交易席位费
    invest_as_receiv	float	Y	应收款项类投资
    total_assets	float	Y	资产总计
    lt_borr	float	Y	长期借款
    st_borr	float	Y	短期借款
    cb_borr	float	Y	向中央银行借款
    depos_ib_deposits	float	Y	吸收存款及同业存放
    loan_oth_bank	float	Y	拆入资金
    trading_fl	float	Y	交易性金融负债
    notes_payable	float	Y	应付票据
    acct_payable	float	Y	应付账款
    adv_receipts	float	Y	预收款项
    sold_for_repur_fa	float	Y	卖出回购金融资产款
    comm_payable	float	Y	应付手续费及佣金
    payroll_payable	float	Y	应付职工薪酬
    taxes_payable	float	Y	应交税费
    int_payable	float	Y	应付利息
    div_payable	float	Y	应付股利
    oth_payable	float	Y	其他应付款
    acc_exp	float	Y	预提费用
    deferred_inc	float	Y	递延收益
    st_bonds_payable	float	Y	应付短期债券
    payable_to_reinsurer	float	Y	应付分保账款
    rsrv_insur_cont	float	Y	保险合同准备金
    acting_trading_sec	float	Y	代理买卖证券款
    acting_uw_sec	float	Y	代理承销证券款
    non_cur_liab_due_1y	float	Y	一年内到期的非流动负债
    oth_cur_liab	float	Y	其他流动负债
    total_cur_liab	float	Y	流动负债合计
    bond_payable	float	Y	应付债券
    lt_payable	float	Y	长期应付款
    specific_payables	float	Y	专项应付款
    estimated_liab	float	Y	预计负债
    defer_tax_liab	float	Y	递延所得税负债
    defer_inc_non_cur_liab	float	Y	递延收益-非流动负债
    oth_ncl	float	Y	其他非流动负债
    total_ncl	float	Y	非流动负债合计
    depos_oth_bfi	float	Y	同业和其它金融机构存放款项
    deriv_liab	float	Y	衍生金融负债
    depos	float	Y	吸收存款
    agency_bus_liab	float	Y	代理业务负债
    oth_liab	float	Y	其他负债
    prem_receiv_adva	float	Y	预收保费
    depos_received	float	Y	存入保证金
    ph_invest	float	Y	保户储金及投资款
    reser_une_prem	float	Y	未到期责任准备金
    reser_outstd_claims	float	Y	未决赔款准备金
    reser_lins_liab	float	Y	寿险责任准备金
    reser_lthins_liab	float	Y	长期健康险责任准备金
    indept_acc_liab	float	Y	独立账户负债
    pledge_borr	float	Y	其中:质押借款
    indem_payable	float	Y	应付赔付款
    policy_div_payable	float	Y	应付保单红利
    total_liab	float	Y	负债合计
    treasury_share	float	Y	减:库存股
    ordin_risk_reser	float	Y	一般风险准备
    forex_differ	float	Y	外币报表折算差额
    invest_loss_unconf	float	Y	未确认的投资损失
    minority_int	float	Y	少数股东权益
    total_hldr_eqy_exc_min_int	float	Y	股东权益合计(不含少数股东权益)
    total_hldr_eqy_inc_min_int	float	Y	股东权益合计(含少数股东权益)
    total_liab_hldr_eqy	float	Y	负债及股东权益总计
    lt_payroll_payable	float	Y	长期应付职工薪酬
    oth_comp_income	float	Y	其他综合收益
    oth_eqt_tools	float	Y	其他权益工具
    oth_eqt_tools_p_shr	float	Y	其他权益工具(优先股)
    lending_funds	float	Y	融出资金
    acc_receivable	float	Y	应收款项
    st_fin_payable	float	Y	应付短期融资款
    payables	float	Y	应付款项
    hfs_assets	float	Y	持有待售的资产
    hfs_sales	float	Y	持有待售的负债
    cost_fin_assets	float	Y	以摊余成本计量的金融资产
    fair_value_fin_assets	float	Y	以公允价值计量且其变动计入其他综合收益的金融资产
    cip_total	float	Y	在建工程(合计)(元)
    oth_pay_total	float	Y	其他应付款(合计)(元)
    long_pay_total	float	Y	长期应付款(合计)(元)
    debt_invest	float	Y	债权投资(元)
    oth_debt_invest	float	Y	其他债权投资(元)
    oth_eq_invest	float	N	其他权益工具投资(元)
    oth_illiq_fin_assets	float	N	其他非流动金融资产(元)
    oth_eq_ppbond	float	N	其他权益工具:永续债(元)
    receiv_financing	float	N	应收款项融资
    use_right_assets	float	N	使用权资产
    lease_liab	float	N	租赁负债
    contract_assets	float	Y	合同资产
    contract_liab	float	Y	合同负债
    accounts_receiv_bill	float	Y	应收票据及应收账款
    accounts_pay	float	Y	应付票据及应付账款
    oth_rcv_total	float	Y	其他应收款(合计)（元）
    fix_assets_total	float	Y	固定资产(合计)(元)
    update_flag	str	Y	更新标识
    """
    
    backfill_balancesheet_data_1()

def backfill_income_data_1():
    """
    补全财务数据
    """
    stock_list = get_stock_list()
    client = get_tushare_client()

    start_date = get_stock_start_date()
    end_date = current_date_ymd()

    for index, row in stock_list.iterrows():
        ts_code = row['ts_code']

        if table_exists_and_not_empty(INCOME_TABLE):
            query = f"SELECT COUNT(*) as count FROM {INCOME_TABLE} WHERE ts_code = '{ts_code}'"
            df = pd.read_sql(query, mysql_engine())
            if df is not None and not df.empty:
                count = df['count'].iloc[0]
                if count > 0:
                    logger.info(f"[财务数据] {ts_code} 的财务数据已存在，跳过")
                    continue
        time.sleep(1)   
        df = client.income(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is not None and not df.empty:
            df.to_sql(INCOME_TABLE, con=mysql_engine(), if_exists='append', index=False)
        logger.info(f"[财务数据] 获取到 {len(df)} 条 {ts_code} 的财务数据")

def backfill_income_data_2():
    """
    补全财务数据
    """
    client = get_tushare_client()
    
    start_date = get_stock_start_date()
    end_date = current_date_ymd()
    quarter_end_dates = get_all_quarter_end(start_date, end_date)
    for date in quarter_end_dates:
        df = client.income_vip(period=date)
        if df is not None and not df.empty:
            df.to_sql(INCOME_TABLE, con=mysql_engine(), if_exists='append', index=False)
        logger.info(f"[财务数据] 获取到 {len(df)} 条 {date} 的财务数据")


def backfill_balancesheet_data_1():
    """
    补全资产负债表数据
    """
    stock_list = get_stock_list()
    client = get_tushare_client()
    
    start_date = get_stock_start_date()
    end_date = current_date_ymd()
    
    for index, row in stock_list.iterrows():
        ts_code = row['ts_code']
        if table_exists_and_not_empty(BALANCESHEET_TABLE):
            query = f"SELECT COUNT(*) as count FROM {BALANCESHEET_TABLE} WHERE ts_code = '{ts_code}'"
            df = pd.read_sql(query, mysql_engine())
            if df is not None and not df.empty:
                count = df['count'].iloc[0]
                if count > 0:
                    logger.info(f"[资产负债表数据] {ts_code} 的资产负债表数据已存在，跳过")
                    continue
        time.sleep(1)
        df = client.balancesheet(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is not None and not df.empty:
            df.to_sql(BALANCESHEET_TABLE, con=mysql_engine(), if_exists='append', index=False)
        logger.info(f"[资产负债表数据] 获取到 {len(df)} 条 {ts_code} 的资产负债表数据")

def get_trade_calendar_list() -> pd.DataFrame:
    """
    获取交易日历列表
    """
    query = f"SELECT * FROM {TRADE_CALENDAR_TABLE}"
    df = pd.read_sql(query, mysql_engine())
    return df


def main():
    """命令行入口点"""
    parser = argparse.ArgumentParser(description="Tushare数据补全")
    parser.add_argument("--income", action="store_true", help="补全财务数据")
    args = parser.parse_args()
    
    if args.income:
        backfill_income_data()

if __name__ == "__main__":
    main()