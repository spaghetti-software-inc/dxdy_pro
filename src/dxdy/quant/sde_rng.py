# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)
#
# Stochastic Differential Equation (SDE) random number generator (RNG). Spaghetti style.
#

import math
import random
import numpy as np
import pandas as pd

import time

from ..settings import Settings
from ..db.utils import get_t_plus_one_cob_date



# not be confused with chicken stock or beef stock, this is stock market data
def insert_random_stock_market_datas(progress):
    tplus1 = get_t_plus_one_cob_date()
    
    db_conn = Settings().get_db_connection(readonly=False)
    
    qry = f"SELECT * FROM securities WHERE security_type_2 = 'Common Stock'"
    securities = db_conn.execute(qry).fetch_df()
    
    market_datas = []
    
    task = progress.add_task("Generating cash equities market data", total=securities.shape[0])
    for security in securities.itertuples():
        qry = f"""
        SELECT
            close_price
        FROM
            market_data
        WHERE
            security_id = {security.security_id}
        AND
            trade_date = (SELECT MAX(trade_date) FROM market_data WHERE security_id = {security.security_id})
        """
        last_close_price_df = db_conn.execute(qry).fetch_df()
        last_close_price = last_close_price_df.iloc[0]['close_price']
        
        
        qry = f"""
        SELECT 
            cob_date AS trade_date 
        FROM 
            calendar 
        WHERE
            cob_date > (SELECT MAX(trade_date) FROM market_data WHERE security_id = {security.security_id})
        AND
            cob_date < '{tplus1}'
        ORDER BY 
            cob_date
        """
        dates = db_conn.execute(qry).fetch_df()
        
        if len(dates) == 0:
            progress.update(task, advance=1)
            continue
        
        market_data = dates
        market_data['security_id'] = security.security_id
        
        len_market_data = market_data.shape[0]
        
        # Using vectorization
        dt = 1.0 / 252.0
        sigma = 0.20  # e.g. 20% annualized volatility
        sigma_sqrt_dt = sigma * np.sqrt(dt)        
        
        Z = np.random.normal(0, 1, len_market_data)
        increments = (0 - 0.5 * sigma**2) * dt + sigma_sqrt_dt * Z  # Per step
        r_t = np.cumsum(increments)  # Sum over all steps
        market_data['close_price'] = last_close_price * np.exp(r_t)

        market_datas.append(market_data)
        
        progress.update(task, advance=1)

    market_datas_df = pd.concat(market_datas)

    # insert the market data
    db_conn.register('tmp_market_data', market_datas_df)
        
    qry = f"""
    INSERT INTO 
        market_data (security_id, trade_date, close_price)
    SELECT 
        security_id, trade_date, close_price
    FROM 
        tmp_market_data
    """
    db_conn.execute(qry)

    db_conn.unregister('tmp_market_data')
        
    
    db_conn.commit()
    db_conn.close()
      
      

                      
# Ornstein–Uhlenbeck Euler–Maruyama Discretization
# https://en.wikipedia.org/wiki/Ornstein%E2%80%93Uhlenbeck_process
def simulate_ou_discrete_vectorized(n, dt, X0, mu, theta, sigma):
    """
    Vectorized discrete-time OU process using the recursion:
        X_{k+1} = (1 - theta*dt)*X_k + theta*mu*dt + sigma*sqrt(dt)*Z_k
        and the convolution calculus.
    """
    # Define parameters
    alpha = 1.0 - theta*dt  # coefficient on X_k
    b = theta * mu * dt
    c = sigma * np.sqrt(dt)
    
    # Prepare arrays
    Z = np.random.normal(size=n)  # Z_k
    X = np.zeros(n)
    
    # The power of alpha^k for k=0..(n-1)
    alpha_powers = np.power(alpha, np.arange(n))  # [1, alpha, alpha^2, ...]
    
    # 1) The homogeneous solution: alpha^k * X0
    # 2) The part from b: b * sum_{j=0}^{k-1} alpha^{k-1-j}
    # 3) The part from c * Z_j: c * sum_{j=0}^{k-1} alpha^{k-1-j} Z_j
    #
    # We'll handle 2) and 3) via convolution with alpha^m reversed.

    # (A) Convolution for the random part c * Z_j
    # We want sum_{j=0}^{k} alpha^{k-j} Z_j, which is the discrete convolution of alpha^m with Z.
    conv_z = np.convolve(alpha_powers, Z, mode='full')[:n]
    random_part = c * conv_z
    
    # (B) Convolution for the constant part b
    # sum_{j=0}^{k-1} alpha^{k-1-j} = alpha^{k-1} + alpha^{k-2} + ... + alpha^0
    # That sum is actually (1 - alpha^k)/(1 - alpha) if alpha != 1.
    # We can also do it by convolving alpha^m with a constant sequence [1,1,1,...].
    ones = np.ones(n)
    conv_b = np.convolve(alpha_powers, ones, mode='full')[:n]
    constant_part = b * conv_b
    
    # Putting it all together for each k:
    # X_k = alpha^k * X0  +  constant_part[k]  +  random_part[k]
    X = alpha_powers * X0 + constant_part + random_part
    
    return X


# what is a numéraire? it is a currency that is used as a reference point for market prices.
# in this case, we are using USD as the numéraire.
# we could also use wheels of Parmigiano Reggiano cheese as the numéraire, but it's not as practical.
# we could use Dodge Coin as the numéraire, but it is so unstable that it would make 
# Parmigiano Reggiano cheese look like a practical numéraire.
def insert_random_fx_rates_datas(progress):
    tplus1 = get_t_plus_one_cob_date()
    
    db_conn = Settings().get_db_connection(readonly=False)
    
    qry = f"SELECT * FROM currencies"
    numéraires = db_conn.execute(qry).fetch_df()
    
    task = progress.add_task("Generating FX rates market data", total=numéraires.shape[0])
    for numéraire in numéraires.itertuples():
        qry = f"""
        SELECT
            fx_rate
        FROM
            fx_rates_data
        WHERE
            ccy = '{numéraire.ccy}'
        AND
            fx_date = (SELECT MAX(fx_date) FROM fx_rates_data WHERE ccy = '{numéraire.ccy}')
        """
        last_numéraire_rate_df = db_conn.execute(qry).fetch_df()
        last_numéraire_rate = last_numéraire_rate_df.iloc[0]['fx_rate']
        
        
        qry = f"""
        SELECT 
            cob_date AS fx_date 
        FROM 
            calendar 
        WHERE
            cob_date > (SELECT MAX(fx_date) FROM fx_rates_data WHERE ccy = '{numéraire.ccy}')
        AND
            cob_date < '{tplus1}'            
        ORDER BY 
            cob_date
        """
        dates = db_conn.execute(qry).fetch_df()
        
        if len(dates) == 0:
            progress.update(task, advance=1)
            continue
        
        
        dt = 1.0 / 252.0
        mu = 1.0  # mean reversion level (could be anything you'd like)
        theta = 2.0  # speed of reversion
        sigma = 0.1  # volatility
        sigma_sqrt_dt = sigma * np.sqrt(dt)

        fx_rates = dates.reset_index(drop=True)

        
        fx_rates['ccy'] = numéraire.ccy
        
        len_market_data = fx_rates.shape[0]
        
        if numéraire.ccy != 'USD':
            fx_rates['fx_rate'] = simulate_ou_discrete_vectorized(len_market_data, dt, last_numéraire_rate, mu, theta, sigma_sqrt_dt)

        else:
            fx_rates['fx_rate'] = 1.0
            
        db_conn.register('tmp_fx_rates', fx_rates)
        
        qry = f"""
        INSERT INTO fx_rates_data
            (fx_date, ccy, fx_rate)
        SELECT
            fx_date, ccy, fx_rate
        FROM
            tmp_fx_rates
        """
        db_conn.execute(qry)
        
        db_conn.unregister('tmp_fx_rates')
        
        
        progress.update(task, advance=1)
    
    
    db_conn.commit()
    db_conn.close()