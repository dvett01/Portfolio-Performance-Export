import lxml.etree as et
import pandas as pd


class PortfolioPerformanceFile:
    def __init__ (self, filepath):
        self.filepath = filepath
        self.root = et.parse(filepath)
        self.securities = []
        
        
    def check_for_ref_lx(self, element):
        ref = element.attrib.get("reference")
        while ref is not None:
            element=self.root.find(self.root.getelementpath(element)+"/"+ref)
            ref = element.attrib.get("reference")
        return element
        
    def get_df_securities(self):
        dfcols = ['idx','uuid', 'name', 'ticker', 'isin', "wkn","cur"]
        df = pd.DataFrame(columns=dfcols)
        for idx, security in enumerate(self.root.findall(".//securities/security")):
            if security is not None:
                    sec_idx = idx + 1
                    sec_uuid =  security.find('uuid').text if security.find('uuid') is not None else ""
                    sec_name =  security.find('name').text if security.find('name') is not None else ""
                    sec_isin =  security.find('isin').text if security.find('isin') is not None else ""
                    sec_wkn =  security.find('wkn').text if security.find('wkn') is not None else ""
                    sec_curr = security.find('currencyCode').text if security.find('currencyCode') is not None else ""
                    sec_ticker = security.find('tickerSymbol').text if security.find('tickerSymbol') is not None else ""
                    df = df.append(pd.Series([sec_idx, sec_uuid, sec_name,sec_ticker, sec_isin,sec_wkn, sec_curr], index=dfcols),ignore_index=True)
        return df
    
    def get_df_all_prices(self):
        dfcols = ['date','price', 'isin']
        df = pd.DataFrame(columns=dfcols)
        for idx, security in enumerate(self.root.findall(".//securities/security")):
            sec_isin =  security.find('isin').text
            for price in security.findall(".//prices/price"):
                date = price.attrib.get("t")
                price = price.attrib.get("v")
                df = df.append(pd.Series([date, price, sec_isin], index=dfcols),ignore_index=True)
        df = df.set_index(["date","isin"]).unstack().price
        return df
    
    def get_df_portfolios(self):
        dfcols = ['idx','uuid', 'name', 'currencycode', 'isretiredxpath']
        df = pd.DataFrame(columns=dfcols)           
        for idx, portfolio in enumerate(self.root.findall(".//portfolios/")):
            portfolio = self.check_for_ref_lx(portfolio)
            ptf_idx = idx + 1 
            ptf_uuid = portfolio.find('uuid').text if portfolio.find('uuid') is not None else ""
            ptf_name =  portfolio.find('name').text if portfolio.find('name') is not None else ""
            ptf_currencycode = portfolio.find("currencyCode").text if portfolio.find('currencyCode') is not None else ""
            ptf_isretired = portfolio.find("isRetired").text if portfolio.find('isRetired') is not None else ""
            df = df.append(pd.Series([ptf_idx, ptf_uuid, ptf_name,ptf_currencycode, ptf_isretired], index=dfcols),ignore_index=True)
        return df
    
    def get_df_accounts(self):
        dfcols = ['idx','uuid', 'name', 'currencycode', 'isretiredxpath', "xpath"]
        df = pd.DataFrame(columns=dfcols)               
        for idx, account in enumerate(self.root.findall('.//accounts/account')):
            account = self.check_for_ref_lx(account)
            acc_idx = idx + 1
            acc_uuid = account.find('uuid').text if account.find('uuid') is not None else ""
            acc_name = account.find('name').text if account.find('name') is not None else ""
            acc_currencycode = account.find('currencycode').text if account.find('currencycode') is not None else ""
            acc_isretired = account.find('isretiredxpath').text if account.find('isretiredxpath') is not None else ""
            acc_xpath = f".//accounts/account[{idx + 1}]"
            df = df.append(pd.Series([acc_idx, acc_uuid, acc_name,acc_currencycode, acc_isretired,acc_xpath], index=dfcols),ignore_index=True)
        return df
    
    
    def subtree_sum(self, element, attrib):
        summe = 0
        for i in element:
            summe= summe + float(i.attrib[attrib])/100
        return summe
    
    
    def get_transactions(self):
        cross_ptf_ta = []
        acct_ta = []
        for account in self.root.findall("./accounts/account"): 
            account = self.check_for_ref_lx(account)
            account_uuid = account.find("uuid").text
            account_name = account.find("name").text
            account_currencyCode = account.find("currencyCode").text
            
            for account_transaction in account.findall("transactions/account-transaction"): 
                account_transaction = self.check_for_ref_lx(account_transaction)
                account_transaction_uuid = account_transaction.find("uuid").text
                account_transaction_date = account_transaction.find("date").text
                account_transaction_currencyCode = account_transaction.find("currencyCode").text
                account_transaction_amount = float(account_transaction.find("amount").text)/100
                account_transaction_shares = float(account_transaction.find("shares").text)/100000000
                account_transaction_type = account_transaction.find("type").text
                account_transaction_note = account_transaction.find('note').text if account_transaction.find('note') is not None else ""
                account_transaction_source = account_transaction.find('source').text if account_transaction.find('source') is not None else ""
                account_transaction_security = self.check_for_ref_lx(account_transaction.find("security")).find("uuid").text if account_transaction.find("security") is not None else ""
                account_transaction_fee = float(account_transaction.find('units/unit[@type="FEE"]/amount').attrib["amount"])/100 if account_transaction.find('units/unit[@type="FEE"]/amount') is not None else 0
                account_transaction_fee_iso = account_transaction.find('units/unit[@type="FEE"]/amount').attrib["currency"] if account_transaction.find('units/unit[@type="FEE"]/amount') is not None else ""
                account_transaction_tax= float(self.subtree_sum(account_transaction.findall('units/unit[@type="TAX"]/amount'),"amount")) if account_transaction.findall('units/unit[@type="TAX"]/amount') is not None else 0
                account_transaction_tax_iso = account_transaction.find('units/unit[@type="TAX"]/amount').attrib["currency"] if account_transaction.find('units/unit[@type="TAX"]/amount') is not None else ""
                
                account_transaction_gross_value_amount = float(account_transaction.find('units/unit[@type="GROSS_VALUE"]/amount').attrib["amount"])/100 if account_transaction.find('units/unit[@type="GROSS_VALUE"]/amount') is not None else 0
                account_transaction_gross_value_amount_iso = account_transaction.find('units/unit[@type="GROSS_VALUE"]/amount').attrib["currency"] if account_transaction.find('units/unit[@type="GROSS_VALUE"]/amount') is not None else ""
                account_transaction_gross_value_amount_forex = float(account_transaction.find('units/unit[@type="GROSS_VALUE"]/forex').attrib["amount"])/100 if account_transaction.find('units/unit[@type="GROSS_VALUE"]/forex') is not None else 0
                account_transaction_gross_value_amount_forex_iso = account_transaction.find('units/unit[@type="GROSS_VALUE"]/forex').attrib["currency"] if account_transaction.find('units/unit[@type="GROSS_VALUE"]/forex') is not None else ""
                account_transaction_gross_value_exchangeRate = float(account_transaction.find('units/unit[@type="GROSS_VALUE"]/exchangeRate').text) if account_transaction.find('units/unit[@type="GROSS_VALUE"]/exchangeRate') is not None else 0
                
                acct_ta.append([
                    account_uuid, "", "",account_transaction_uuid, account_transaction_date, account_transaction_currencyCode, account_transaction_amount, account_transaction_shares,
                    account_transaction_type, account_transaction_note, account_transaction_source, account_transaction_security,
                    account_transaction_fee, account_transaction_fee_iso,
                    account_transaction_tax, account_transaction_tax_iso,
                    account_transaction_gross_value_amount, account_transaction_gross_value_amount_iso,
                    account_transaction_gross_value_amount_forex, account_transaction_gross_value_amount_forex_iso,
                    account_transaction_gross_value_exchangeRate
                ])
                
                
                for crossEntry in account_transaction.findall("crossEntry"):
                    crossEntry = self.check_for_ref_lx(crossEntry)
                    crossEntry_account_ref = self.check_for_ref_lx(crossEntry.find("account")).find("uuid").text if crossEntry.find("account") is not None else ""
                    
                    
    
                for portfolio in account_transaction.findall("crossEntry/portfolio"):
                    portfolio = self.check_for_ref_lx(portfolio)
                    portfolio_uuid = portfolio.find("uuid").text
                                        
                    for portfolio_transaction in portfolio.findall("transactions/portfolio-transaction"):
                        portfolio_transaction = self.check_for_ref_lx(portfolio_transaction)
                        portfolio_transaction_uuid = portfolio_transaction.find("uuid").text
                        portfolio_transaction_date = portfolio_transaction.find("date").text
                        portfolio_transaction_currencyCode = portfolio_transaction.find("currencyCode").text
                        portfolio_transaction_amount = float(portfolio_transaction.find("amount").text)/100
                        portfolio_transaction_shares = float(portfolio_transaction.find("shares").text)/100000000
                        portfolio_transaction_type = portfolio_transaction.find("type").text
                        portfolio_transaction_note = portfolio_transaction.find('note').text if portfolio_transaction.find('note') is not None else ""
                        portfolio_transaction_source = portfolio_transaction.find('source').text if portfolio_transaction.find('source') is not None else ""
                        portfolio_transaction_security = self.check_for_ref_lx(portfolio_transaction.find("security")).find("uuid").text if portfolio_transaction.find("security") is not None else ""
                        portfolio_transaction_fee = float(portfolio_transaction.find('units/unit[@type="FEE"]/amount').attrib["amount"])/100 if portfolio_transaction.find('units/unit[@type="FEE"]/amount') is not None else 0
                        portfolio_transaction_fee_iso = portfolio_transaction.find('units/unit[@type="FEE"]/amount').attrib["currency"] if portfolio_transaction.find('units/unit[@type="FEE"]/amount') is not None else ""
                        portfolio_transaction_tax= float(self.subtree_sum(portfolio_transaction.findall('units/unit[@type="TAX"]/amount'),"amount")) if portfolio_transaction.findall('units/unit[@type="TAX"]/amount') is not None else 0
                        portfolio_transaction_tax_iso = portfolio_transaction.find('units/unit[@type="TAX"]/amount').attrib["currency"] if portfolio_transaction.find('units/unit[@type="TAX"]/amount') is not None else ""
                        
                        portfolio_transaction_gross_value_amount = float(portfolio_transaction.find('units/unit[@type="GROSS_VALUE"]/amount').attrib["amount"])/100 if portfolio_transaction.find('units/unit[@type="GROSS_VALUE"]/amount') is not None else 0
                        portfolio_transaction_gross_value_amount_iso = portfolio_transaction.find('units/unit[@type="GROSS_VALUE"]/amount').attrib["currency"] if portfolio_transaction.find('units/unit[@type="GROSS_VALUE"]/amount') is not None else ""
                        portfolio_transaction_gross_value_amount_forex = float(portfolio_transaction.find('units/unit[@type="GROSS_VALUE"]/forex').attrib["amount"])/100 if portfolio_transaction.find('units/unit[@type="GROSS_VALUE"]/forex') is not None else 0
                        portfolio_transaction_gross_value_amount_forex_iso = portfolio_transaction.find('units/unit[@type="GROSS_VALUE"]/forex').attrib["currency"] if portfolio_transaction.find('units/unit[@type="GROSS_VALUE"]/forex') is not None else ""
                        portfolio_transaction_gross_value_exchangeRate = float(portfolio_transaction.find('units/unit[@type="GROSS_VALUE"]/exchangeRate').text) if portfolio_transaction.find('units/unit[@type="GROSS_VALUE"]/exchangeRate') is not None else 0
                        
                        cross_ptf_ta.append([account_uuid,
                                   crossEntry_account_ref,
                                   portfolio_uuid,
                                   portfolio_transaction_uuid, 
                                   portfolio_transaction_date, portfolio_transaction_currencyCode, portfolio_transaction_amount, portfolio_transaction_shares,
                                   portfolio_transaction_type, portfolio_transaction_note, portfolio_transaction_source, portfolio_transaction_security, 
                                   portfolio_transaction_fee, portfolio_transaction_fee_iso, 
                                   portfolio_transaction_tax, portfolio_transaction_tax_iso,
                                   portfolio_transaction_gross_value_amount, portfolio_transaction_gross_value_amount_iso,
                                   portfolio_transaction_gross_value_amount_forex, portfolio_transaction_gross_value_amount_forex_iso,
                                   portfolio_transaction_gross_value_exchangeRate])

        cols=["account_uuid", 
                                "crossEntry_account_ref",
                                "portfolio_uuid",
                                   "portfolio_transaction_uuid", 
                                   "portfolio_transaction_date", "portfolio_transaction_currencyCode", "portfolio_transaction_amount", "portfolio_transaction_shares",
                                   "portfolio_transaction_type", "portfolio_transaction_note", "portfolio_transaction_source", "portfolio_transaction_security", 
                                   "portfolio_transaction_fee", "portfolio_transaction_fee_iso", 
                                   "portfolio_transaction_tax", "portfolio_transaction_tax_iso",
                                   "portfolio_transaction_gross_value_amount", "portfolio_transaction_gross_value_amount_iso",
                                   "portfolio_transaction_gross_value_amount_forex", "portfolio_transaction_gross_value_amount_forex_iso",
                                   "portfolio_transaction_gross_value_exchangeRate"
                        ]
        acct_ta = pd.DataFrame(acct_ta, columns=cols)
        cross_ptf_ta = pd.DataFrame(cross_ptf_ta, columns=cols)
        
        ta = pd.concat([acct_ta, cross_ptf_ta],axis=0)
        
        securities = self.get_df_securities().set_index("uuid")
        accounts = self.get_df_accounts().set_index("uuid")
        portfolios = self.get_df_portfolios().set_index("uuid")
        ta["account_name"] = ta["account_uuid"].map(accounts["name"])
        ta["portfolio_name"] = ta["portfolio_uuid"].map(portfolios["name"])
        ta["crossEntry_account_ref_name"] = ta["crossEntry_account_ref"].map(accounts["name"])
        ta["security_name"] = ta["portfolio_transaction_security"].map(securities["name"])
        ta["date"] = pd.to_datetime(ta["portfolio_transaction_date"],format="%Y-%m-%dT%H:%M")
        ta["Kurs"] = ta["portfolio_transaction_amount"]/ ta["portfolio_transaction_shares"]
        return ta
    
    
    
PP = PortfolioPerformanceFile(filepath="All Portolios.xml")
ta = PP.get_transactions()
