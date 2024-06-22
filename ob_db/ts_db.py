from datetime import date

from ob_db.db_connector import Db, DB_LAKE
from ob_db.dataclass import TsStock


class TsDb(Db):
    """ insert """
    def insert_ts_stock(self, stock: TsStock):
        sql_insert = f"""
        INSERT INTO {DB_LAKE}.ts_stock (
            ts_id, stock_code, stock_name, market_type, closing_price, 
            price_change, price_change_rate, opening_price, high_price, 
            low_price, trade_volume, trade_amount, market_cap, listed_shares
        ) VALUES (
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, 
            %s, %s, %s
        ) ON DUPLICATE KEY UPDATE
        stock_name = VALUES(stock_name),
        closing_price = VALUES(closing_price),
        price_change = VALUES(price_change),
        price_change_rate = VALUES(price_change_rate),
        opening_price = VALUES(opening_price),
        high_price = VALUES(high_price),
        low_price = VALUES(low_price),
        trade_volume = VALUES(trade_volume),
        trade_amount = VALUES(trade_amount),
        market_cap = VALUES(market_cap),
        listed_shares = VALUES(listed_shares);
        """
        data = (
            stock.ts_id, stock.stock_code, stock.stock_name, stock.market_type,
            stock.closing_price, stock.price_change, stock.price_change_rate,
            stock.opening_price, stock.high_price, stock.low_price,
            stock.trade_volume, stock.trade_amount, stock.market_cap, stock.listed_shares
        )

        cursor = self.cnx.cursor()
        cursor.execute(sql_insert, data)
        cursor.close()

    def insert_ts_data(self, price_date: date, stock: TsStock):
        sql_insert = f"""
            INSERT IGNORE INTO {DB_LAKE}.ts_data_{stock.market_type.lower()} (
                ts_id, stock_code, price_date, stock_name, closing_price, 
                price_change, price_change_rate, opening_price, high_price, 
                low_price, trade_volume, trade_amount, market_cap, listed_shares
            ) VALUES (
                %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, 
                %s, %s
            )
        """
        data = (
            stock.ts_id, stock.stock_code,
            price_date, stock.stock_name,
            stock.closing_price, stock.price_change, stock.price_change_rate,
            stock.opening_price, stock.high_price, stock.low_price,
            stock.trade_volume, stock.trade_amount, stock.market_cap, stock.listed_shares
        )

        cursor = self.cnx.cursor()
        cursor.execute(sql_insert, data)
        cursor.close()

    """ select """
    def get_ts_stock(self):
        pass
