import psycopg2
from datetime import datetime
import date
from db_util import db201exec, getconn
def do_trade_date(t_fund_name, hold_day, t_start_date_time, new_old, t_country):
    start_date_time = t_start_date_time     
    #start_date_time = "2020-10-19 00:00:00";
    country = t_country; #For Market
    da_container = []; #For Old Date

    pg_host = '10.188.200.16'
    pg_port = '5432'
    pg_user = "crontab"
    pg_passwd = "pocket888"
    concheck_cn = psycopg2.connect(database = t_country, user = pg_user, password = pg_passwd, host= pg_host, port = pg_port) #Create DB Connection
    curcheck_cn = concheck_cn.cursor()

    fund_name = t_fund_name

    if new_old == 'new':
        delete99 = "DELETE FROM model_strategy_stocks_and_weight WHERE STRATEGY_NAME = '"+str(fund_name)+"'; "
        try:
            db201exec(t_country, delete99);
        except Exception as err:
            print(str(err));

    #Get Market Index
    if country == 'tw': Index = 'TWA00 Index';
    if country == 'us': Index = 'NQ1 Index';

    pool_container = []
    curcheck_cn.execute(" SELECT DISTINCT CAST(DA AS DATE) FROM model_strategy_stocks_and_weight WHERE strategy_name = '"+str(fund_name)+"' ORDER BY DA DESC ");
    curcheckdata0 = curcheck_cn.fetchone();
    while curcheckdata0 is not None:
        datee = curcheckdata0[0];
        pool_container.append(datee);
        curcheckdata0 = curcheck_cn.fetchone()

    #Get Test Date( StartDate ~ EndDate)
    curcheck_cn.execute(" SELECT CAST(DA AS DATE) FROM PRICE WHERE CODE = '"+str(Index)+"' AND DA > '"+str(start_date_time)+"' ORDER BY DA ASC ");
    curcheckdata0 = curcheck_cn.fetchone();
    while curcheckdata0 is not None:
        datee = curcheckdata0[0];
        da_container.append(datee);
        curcheckdata0 = curcheck_cn.fetchone()

    now = date.today()
    #da_container.append(now)
    da_container = set(da_container)
    da_container = sorted(da_container)
    len_date = 0; loop_date = []
    curcheck_cn.execute(" SELECT CAST(DA AS DATE) FROM TRADE_CAL WHERE DA> '"+str(now)+"' ORDER BY DA ASC LIMIT 1 ");
    curcheckdata0 = curcheck_cn.fetchone();
    if curcheckdata0 is not None:
        trade_date = curcheckdata0[0];
    da_container.append(trade_date)

    if len(pool_container) != 0:
        da_container = []
        #Get Test Date( StartDate ~ EndDate)
        curcheck_cn.execute(" SELECT CAST(DA AS DATE) FROM PRICE WHERE CODE = '"+str(Index)+"' AND DA >= (SELECT MAX(DA) FROM model_strategy_stocks_and_weight WHERE strategy_name = '"+str(fund_name)+"') ORDER BY DA ASC ");
        curcheckdata0 = curcheck_cn.fetchone();
        while curcheckdata0 is not None:
            datee = curcheckdata0[0];
            da_container.append(datee);
            curcheckdata0 = curcheck_cn.fetchone()

        now = date.today()
        da_container = set(da_container)
        da_container = sorted(da_container)
        len_date = 0; loop_date = []
        curcheck_cn.execute(" SELECT CAST(DA AS DATE) FROM TRADE_CAL WHERE DA> '"+str(now)+"' ORDER BY DA ASC LIMIT 1 ");
        curcheckdata0 = curcheck_cn.fetchone();
        if curcheckdata0 is not None:
            trade_date = curcheckdata0[0];
        da_container.append(trade_date)


    count = 0; loop_date = []
    for real_date in da_container:
        count = count + 1;
        if len(loop_date) == 0 : count = int(hold_day);
        if count == int(hold_day):
            count = 0;
            if len(pool_container) == 0:
                loop_date.append(real_date)
            else:
                loop_date.append(real_date)

    for x in pool_container:
        try:
            loop_date.remove(x)
        except Exception as err:
            print(str(err))

    return loop_date, trade_date, curcheck_cn, concheck_cn
	
def calculate_strategy_nav_tw_fund(fund1, range_date_start, pg_user, pg_passwd, pg_host, pg_port):
    try:
        del_sql = "DELETE FROM MODEL_NAV where strategy_name = '"+str(fund1)+"' ;"
        try:
            db201exec('tw', del_sql);
        except Exception as err:
            print(str(err));

        daily_curcheck = psycopg2.connect(database='tw', user=pg_user, password=pg_passwd, host=pg_host, port=pg_port)
        daily_cuncheck = daily_curcheck.cursor()

        # Fetching available strategies
        fund2_arr = []
        daily_cuncheck.execute("SELECT DISTINCT STRATEGY_NAME FROM model_strategy_stocks_and_weight where strategy_name = '"+str(fund1)+"'")
        RETURN_NAV_check_data = daily_cuncheck.fetchone()
        while RETURN_NAV_check_data is not None:
            fund2_temp = RETURN_NAV_check_data[0]
            fund2_arr.append(fund2_temp)
            RETURN_NAV_check_data = daily_cuncheck.fetchone()

        fund2_arr = [fund1]

        for fund2 in fund2_arr:
            # Fetching start date
            daily_cuncheck.execute(
                "SELECT DA FROM price WHERE code = 'TWA00 Index' AND da >= %s ORDER BY da ASC LIMIT 1",
                (range_date_start,))
            RETURN_NAV_check_data = daily_cuncheck.fetchone()
            if RETURN_NAV_check_data is not None:
                start_date = RETURN_NAV_check_data[0]

            pre_nav = 1
            date_container = [start_date.date()]

            # Fetching dates for trading
            do_trade_da = []
            daily_cuncheck.execute("SELECT DA FROM price WHERE code = 'TWA00 Index' AND da >= %s ORDER BY da ASC",
                                   (range_date_start,))
            RETURN_NAV_check_data = daily_cuncheck.fetchone()
            while RETURN_NAV_check_data is not None:
                da = RETURN_NAV_check_data[0]
                do_trade_da.append(da)
                RETURN_NAV_check_data = daily_cuncheck.fetchone()

            total_rtn = []
            for day1 in do_trade_da:
                #print(day1)
                # SQL query to calculate return for each strategy
                query1 = """
                    SELECT AVG(TTT.RETURN), 
                           (SELECT DA FROM price WHERE code = 'TWA00 Index' AND da > '"""+str(day1)+"""' ORDER BY da ASC LIMIT 1)
                    FROM (
                        SELECT *, 
                               da AS buy_da,
                               (SELECT NAV FROM PRICE_FUND WHERE da = (SELECT DA FROM price WHERE code = 'TWA00 Index' AND da > '"""+str(day1)+"""' ORDER BY da ASC LIMIT 1) AND code = P.code AND da IS NOT NULL ORDER BY da ASC LIMIT 1) / 
                               (SELECT NAV FROM PRICE_FUND WHERE code = P.code AND da = '"""+str(day1)+"""') - 1 AS return,
                               (SELECT DA FROM price WHERE code = 'TWA00 Index' AND da > '"""+str(day1)+"""' ORDER BY da ASC LIMIT 1)
                        FROM model_strategy_stocks_and_weight AS P
                        WHERE P.strategy_name = '"""+str(fund1)+"""'
                          AND da = (SELECT DA FROM model_strategy_stocks_and_weight
                                    WHERE strategy_name = '"""+str(fund1)+"""' AND da <= '"""+str(day1)+"""' ORDER BY da DESC LIMIT 1)
                    ) AS TTT;
                """
                daily_cuncheck.execute(query1)
                RETURN_NAV_check_data = daily_cuncheck.fetchone()
                if RETURN_NAV_check_data is not None:
                    rtn = RETURN_NAV_check_data[0]
                    day1_add1 = RETURN_NAV_check_data[1]

                    if rtn == None or float(rtn) > 1:
                        rtn = 0

                    total_rtn.append([day1_add1, rtn])
                else:
                    total_rtn.append([day1_add1, 0])

            ins_sql = ""
            for data in total_rtn:
                da = data[0]
                rtn = data[1]
                if rtn is None:
                    rtn = 0
                nav = pre_nav * (1 + rtn)

                if da is not None:
                    # Insert calculated NAV into the database
                    ins_sql = ins_sql + "INSERT INTO MODEL_NAV(DA, STRATEGY_NAME, TYPE, CL) VALUES('"+str(da)+"','"+str(fund2)+"','beta','"+str(nav)+"');"
                    pre_nav = nav
            try:
                db201exec('tw', ins_sql);
            except Exception as err:
                print(str(err));

    except psycopg2.Error as e:
        print("Error:", e)

    return nav

def tw_fund(t_start_date_time, t_fund_name, t_days, t_new_old, t_country, factor, and_conditcail, count, cap):
    country = t_country;  start_date_time = t_start_date_time; fund_name = t_fund_name;
    loop_date, trade_date, curcheck_cn, concheck_cn = do_trade_date(fund_name, t_days, start_date_time, t_new_old, t_country)
    if country == 'tw': Index = 'TWA00 Index';
    if country == 'us': Index = 'NQ1 Index';

    ins_sql = ""; ins_sql99 = "";
    for da in loop_date:
        #print("da:"+str(da));
        curcheck_cn.execute(" SELECT DISTINCT TO_CHAR(D.DA, 'YYYY-MM-DD') AS DA "+
                         " FROM PRICE D "+
                         " WHERE 1=1 "+
                         " AND D.CODE = '"+Index+"' "+
                         " AND D.da <= '"+str(da)+"'  "+
                         " ORDER BY DA DESC LIMIT 21 ");
        datacheck = curcheck_cn.fetchone()
        day1 = ""; day2 = ""; day3 = "";
        while datacheck is not None:
            dat = datacheck[0]
            if day1 == "": day1 = dat; tmp_d1 = dat; #Today
            elif day2 == "": day2 = dat; tmp_d2 = dat; #2
            elif day3 == "": day3 = dat; tmp_d3 = dat; #2
            datacheck = curcheck_cn.fetchone()

        if str(da) != str(day1): day2 = day1;

        cn_pool = []
        fof_weight = 1
        tmp_cn_pool = []
        curcheck_cn_1 = concheck_cn.cursor()
        curcheck_cn_1.execute(" SELECT T1.CODE, xt_fund.* FROM( "+
                              " SELECT *, (SELECT COUNT(DA) FROM PRICE_FUND WHERE CODE = fund_performance.CODE AND DA <= '"+str(day3)+"')  "+
                              " FROM fund_performance WHERE DA = (SELECT DISTINCT DA FROM FUND_PERFORMANCE WHERE DA <= '"+str(day3)+"' ORDER BY DA DESC LIMIT 1) AND YEARS = '3'  "+
                              " AND CODE IN (SELECT CODE FROM MAINCODE_FUND WHERE RISK_LEVEL IN ('RR4','RR5') ) "+
                              " AND CODE IN (SELECT CODE FROM PRICE_FUND WHERE DA = '"+str(day3)+"' ) "+
                              " ) AS T1, xt_fund "+
                              " WHERE t1.code = xt_fund.code "+
                              " and xt_fund.da = '"+str(day3)+"' "+
                              " and COUNT > 2000 ORDER BY CR DESC limit 3 ");
        datacheck_cn_1 = curcheck_cn_1.fetchone()
        while datacheck_cn_1 is not None:
            code = datacheck_cn_1[0]
            tmp_cn_pool.append(code)
            datacheck_cn_1 = curcheck_cn_1.fetchone()

        #tmp_cn_pool = ['QQQ']
        if str(trade_date) > day1:
            if datetime.strptime(day1, "%Y-%m-%d").date() in loop_date:
                da = day1
            else:
                da = trade_date;

        if len(tmp_cn_pool) != 0:
            for data in tmp_cn_pool:
                cn_pool_code = data
                weight = float(1) / len(tmp_cn_pool)
                ins_sql99 = ins_sql99 + " INSERT INTO model_strategy_stocks_and_weight(DA, STRATEGY_NAME, CODE, WEIGHT) " \
                                    " VALUES('"+str(da)+"', '"+str(fund_name)+"', '"+str(cn_pool_code)+"', '"+str(weight)+"') ON CONFLICT ON CONSTRAINT model_strategy_stocks_and_weight_pkey DO NOTHING;"
        else:
            ins_sql99 = ins_sql99 + " INSERT INTO model_strategy_stocks_and_weight(DA, STRATEGY_NAME, CODE, WEIGHT) " \
                                " VALUES('"+str(da)+"', '"+str(fund_name)+"', '2330 TT Equity', '0') ON CONFLICT ON CONSTRAINT model_strategy_stocks_and_weight_pkey DO NOTHING;"
    try:
        db201exec(t_country, ins_sql99);
    except Exception as err:
        print(str(err));

def do_mxx_fund():
    tw_fund("2017-01-01 00:00:00", 'M3_fund_of_fund', 20, 'new', 'tw','dt', 'dt>yt', '20', '30')
    calculate_strategy_nav_tw_fund('M3_fund_of_fund', '2017-01-04', 'crontab', 'itolemma888', '10.188.200.16', 5432)

if __name__ == '__main__':
    do_mxx_fund()