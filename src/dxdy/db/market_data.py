# Copyright (C) 2025 Spaghetti Software Inc. (SPGI)

from datetime import date

from loguru import logger
import rich

import dxdy.bbg 
import dxdy.bbg.api
import dxdy.quant
import dxdy.quant.api
import dxdy.yf
import dxdy.yf.api

import dxdy.db.utils as db_utils
from dxdy.settings import Settings


class MarketDataApi:
    def __init__(self):
        pass
    
    def real_time_api(self, tickers):
        raise NotImplementedError

    def timeseries_market_data_api(self, db, figis, start_date : date, cob_date : date, tplus_one : date) -> None:
        raise NotImplementedError

    def timeseries_div_splits_data_api(self, db, figis, start_date : date, cob_date : date, tplus_one : date) -> None:
        raise NotImplementedError

    def timeseries_fx_rates_data_api(self, db, start_date : date, cob_date : date, tplus_one : date) -> None:
        raise NotImplementedError

    def load_sector_mappings_data_api(self, db, figis) -> None:
        raise NotImplementedError

    def load_new_options_data_api(self, db, securities_data) -> None:
        raise NotImplementedError

    def load_new_securities_data_api(self, db, figis) -> None:
        raise NotImplementedError

    def load_trade_blotter_api(self, db, cob_date : date) -> None:
        raise NotImplementedError

    def load_intraday_trade_blotter_api(self, cob_date : date) -> None:
        raise NotImplementedError
    
    def securities_identifier(self) -> str:
        return 'figi'
    
    
class BbgMarketDataApi(MarketDataApi):
    def __init__(self):
        logger.info("Using Bloomberg Market Data API")
    
    def real_time_api(self, tickers):
        return dxdy.bbg.api.real_time_api(tickers)
    
    def timeseries_market_data_api(self, db, figis, start_date : date, cob_date : date, tplus_one : date) -> None:
        return dxdy.bbg.api.timeseries_market_data_api(db, figis, start_date, cob_date)

    def timeseries_div_splits_data_api(self, db, figis, start_date : date, cob_date : date, tplus_one : date) -> None:
        return dxdy.bbg.api.timeseries_div_splits_data_api(db, figis, start_date, cob_date)

    def timeseries_fx_rates_data_api(self, db, start_date : date, cob_date : date, tplus_one : date) -> None:
        return dxdy.bbg.api.timeseries_fx_rates_data_api(db, start_date, cob_date)

    def load_sector_mappings_data_api(self, db, figis) -> None:
        return dxdy.bbg.api.load_sector_mappings_data_api(db, figis)

    def load_new_options_data_api(self, db, securities_data) -> None:
        return dxdy.bbg.api.load_new_options_data_api(db, securities_data)

    def load_new_securities_data_api(self, db, figis) -> None:
        return dxdy.bbg.api.load_new_securities_data_api(db, figis)

    def load_trade_blotter_api(self, db, cob_date : date) -> None:
        return dxdy.bbg.api.load_trade_blotter_api(db, cob_date)
        
    def load_intraday_trade_blotter_api(self, cob_date : date) -> None:
        return dxdy.bbg.api.load_intraday_trade_blotter_api(cob_date)
       
        
class SpaghettiQuantMarketDataApi(MarketDataApi):
    def __init__(self):
        logger.info("Using SpaghettiQuant Market Data API")
    
    def real_time_api(self, tickers):
        return dxdy.quant.api.real_time_api(tickers)

    def timeseries_market_data_api(self, db, figis, start_date : date, cob_date : date, tplus_one : date) -> None:
        return dxdy.quant.api.timeseries_market_data_api(db, figis, start_date, cob_date)

    def timeseries_fx_rates_data_api(self, db, start_date : date, cob_date : date, tplus_one : date) -> None:
        return dxdy.quant.api.timeseries_fx_rates_data_api(db, start_date, cob_date)

    def load_sector_mappings_data_api(self, db, figis) -> None:
        return dxdy.quant.api.load_sector_mappings_data_api(db, figis)

    def load_new_options_data_api(self, db, securities_data) -> None:
        return dxdy.quant.api.load_new_options_data_api(db, securities_data)

    def load_new_securities_data_api(self, db, figis) -> None:
        return dxdy.quant.api.load_new_securities_data_api(db, figis)

    def load_trade_blotter_api(self, db, cob_date : date) -> None:
        return dxdy.quant.api.load_trade_blotter_api(db, cob_date)
        
    def load_intraday_trade_blotter_api(self, cob_date : date) -> None:
        return dxdy.quant.api.load_intraday_trade_blotter_api(cob_date)
    
class YahooMarketDataApi(MarketDataApi):
    def __init__(self):
        logger.info("Using Yahoo Market Data API")
    
    def real_time_api(tickers):
        return dxdy.quant.api.real_time_api(tickers)

    def timeseries_market_data_api(self, db, figis, start_date : date, cob_date : date, tplus_one : date) -> None:
        return dxdy.yf.api.timeseries_market_data_api(db, figis, cob_date, tplus_one)

    def timeseries_div_splits_data_api(self, db, figis, start_date : date, cob_date : date, tplus_one : date) -> None:
        return dxdy.yf.api.timeseries_div_splits_data_api(db, figis, cob_date, tplus_one)

    def timeseries_fx_rates_data_api(self, db, start_date : date, cob_date : date, tplus_one : date) -> None:
        return dxdy.yf.api.timeseries_fx_rates_data_api(db, cob_date, tplus_one)

    def load_sector_mappings_data_api(self, db, figis) -> None:
        return dxdy.yf.api.load_sector_mappings_data_api(db, figis)

    def load_new_options_data_api(self, db, securities_data) -> None:
        return dxdy.yf.api.load_new_options_data_api(db, securities_data)

    def load_new_securities_data_api(self, db, figis) -> None:
        return dxdy.yf.api.load_new_securities_data_api(db, figis)

    def load_trade_blotter_api(self, db, cob_date : date) -> None:
        return dxdy.quant.api.load_trade_blotter_api(db, cob_date)

    def securities_identifier(self) -> str:
        return 'ticker'

    def load_intraday_trade_blotter_api(self, cob_date : date) -> None:
        return dxdy.quant.api.load_intraday_trade_blotter_api(cob_date)


class MarketDataApiFactory:
    def get_api(self, market_data_provider: str) -> MarketDataApi:
        if market_data_provider == 'bbg':
            return BbgMarketDataApi()
        
        elif market_data_provider == 'yahoo':
            return YahooMarketDataApi()
        
        elif market_data_provider == 'spgi':
            return SpaghettiQuantMarketDataApi()
        
        else:
            raise ValueError(f"Unknown market data provider: {market_data_provider}")
    