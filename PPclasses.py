import lxml.etree as et
import pandas as pd


class PortfolioPerformanceFile:
    def __init__ (self, filepath):
        self.filepath = filepath
        #self.pp_tree = et.parse(filepath)
        #self.root = self.pp_tree.getroot()
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
                    sec_uuid =  security.find('uuid').text
                    sec_name =  security.find('name').text
                    sec_isin =  security.find('isin').text
                    sec_wkn =  security.find('wkn').text
                    sec_curr = security.find('currencyCode').text
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
    
    def get_df_all_account_transactions(self):
        dfcols = ['date',"type",'amount',"cur","shares","isin","wkn","ticker_sym","sym_name", "Acct", "note"]
        df = pd.DataFrame(columns=dfcols)
        for idx, acct in enumerate(self.root.findall(".//accounts/account")):
                acct = self.check_for_ref_lx(acct)
                acct_name =  acct.find('name').text if acct.find('name') is not None else ""
                for ta in acct.findall(".//transactions/account-transaction"):
                    if ta is not None:
                        ta = self.check_for_ref_lx(ta)
                        date = self.check_for_ref_lx(ta.find("date")).text if ta.find('date') is not None else ""
                        shares = ta.find("shares").text if ta.find('shares') is not None else 0
                        amount = ta.find("amount").text if ta.find('amount') is not None else 0
                        cur = ta.find("currencyCode").text if ta.find('currencyCode') is not None else ""
                        note = ta.find("note").text if ta.find('note') is not None else ""
                        isin=self.check_for_ref_lx(ta.find("security")).find("isin").text if ta.find('security') is not None else ""
                        try:
                            wkn=self.check_for_ref_lx(ta.find("security")).find("wkn").text if ta.find('security') is not None else "" 
                        except: wkn=""
                        try:
                            ticker_sym=self.check_for_ref_lx(ta.find("security")).find("tickerSymbol").text if ta.find('security') is not None else ""
                        except: ticker_sym=""
                        sym_name=self.check_for_ref_lx(ta.find("security")).find("name").text if ta.find('security') is not None else ""
                        typez = ta.find("type").text if ta.find('type') is not None else ""
                        df = df.append(pd.Series([date, typez, float(amount)/100, cur, float(shares)/100000000, isin,wkn,ticker_sym,sym_name,acct_name,note], index=dfcols),ignore_index=True)
        return df
    
    def get_df_all_portfolio_transactions(self):
        dfcols = ['date',"type",'amount',"cur","shares","isin","wkn","ticker_sym","sym_name", "Acct", "note"]
        df = pd.DataFrame(columns=dfcols)
        for idx, ptf in enumerate(self.root.findall(".//portfolios/")):
                ptf = self.check_for_ref_lx(ptf)
                ptf_name =  ptf.find('name').text if ptf.find('name') is not None else ""
                for ta in ptf.findall("./transactions"):
                    ta = self.check_for_ref_lx(ta)
                    for ta in ta:
                        date = self.check_for_ref_lx(ta.find("date")).text if ta.find('date') is not None else ""
                        shares = ta.find("shares").text if ta.find('shares') is not None else 0
                        amount = ta.find("amount").text if ta.find('amount') is not None else 0
                        cur = ta.find("currencyCode").text if ta.find('currencyCode') is not None else ""
                        note = ta.find("note").text if ta.find('note') is not None else ""
                        isin=self.check_for_ref_lx(ta.find("security")).find("isin").text if ta.find('security') is not None else ""
                        try:
                            wkn=self.check_for_ref_lx(ta.find("security")).find("wkn").text if ta.find('security') is not None else "" 
                        except: wkn=""
                        try:
                            ticker_sym=self.check_for_ref_lx(ta.find("security")).find("tickerSymbol").text if ta.find('security') is not None else ""
                        except: ticker_sym=""
                        sym_name=self.check_for_ref_lx(ta.find("security")).find("name").text if ta.find('security') is not None else ""
                        typez = ta.find("type").text if ta.find('type') is not None else ""
                        df = df.append(pd.Series([date, typez, float(amount)/100, cur, float(shares)/100000000, isin,wkn,ticker_sym,sym_name,acct_name,note], index=dfcols),ignore_index=True)
        return df
    

        
PP = PortfolioPerformanceFile(filepath="MeinePortfolien.xml")

PP.get_df_all_portfolio_transactions()
