using HtmlAgilityPack;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace FindStock
{
    class Processor
    {
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // | Config                                                          |
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        private static string DatabaseServer = ConfigurationManager.AppSettings["DatabaseServer"];
        private static string Database = ConfigurationManager.AppSettings["Database"];
        private static string Username = ConfigurationManager.AppSettings["DatabaseUsername"];
        private static string Password = ConfigurationManager.AppSettings["DatabasePassword"];
        private static Plog log = new Plog(true);
        public static List<string> set50 = new List<string>();
        public static List<string> set100 = new List<string>();
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // | Model                                                           |
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        private class Stock
        {

            public Stock() { }

            // Properties.
            public string Symbol { get; set; }
            public string Name { get; set; }
            public string Market { get; set; }
            public string Industry { get; set; }
            public string Sector { get; set; }
            public string Website { get; set; }
            public string SET50 { get; set; }
            public string SET100 { get; set; }
            public string Market_cap { get; set; }
            public string First_trade_date { get; set; }
            public string Return_rate { get; set; }
            public string Price_rate { get; set; }
            public string IAA_rate { get; set; }
            public string Growth_stock_rate { get; set; }
            public string Stock_dividend_rate { get; set; }
            public string SET50_rate { get; set; }
            public string SET100_rate { get; set; }
            public string PE_rate { get; set; }
            public string PBV_rate { get; set; }
            public string ROE_rate { get; set; }
            public string ROA_rate { get; set; }
            public string Market_cap_rate { get; set; }
            public string Score { get; set; }
            public string LastUpdate { get; set; }
        }
        private class FinanceInfo
        {

            public FinanceInfo() { }

            // Properties.
            public string Date { get; set; }
            public string Symbol { get; set; }
            public string Year { get; set; }
            public string Quarter { get; set; }
            public string Assets { get; set; }
            public string Liabilities { get; set; }
            public string Equity { get; set; }
            public string Paid_up_cap { get; set; }
            public string Revenue { get; set; }
            public string NetProfit { get; set; }
            public string EPS { get; set; }
            public string ROA { get; set; }
            public string ROE { get; set; }
            public string NetProfitMargin { get; set; }
            public string LastUpdate { get; set; }
        }
        private class FinanceStat
        {

            public FinanceStat() { }

            // Properties.
            public string Date { get; set; }
            public string Symbol { get; set; }
            public string Year { get; set; }
            public string Lastprice { get; set; }
            public string Market_cap { get; set; }
            public string FS_date { get; set; }
            public string PE { get; set; }
            public string PBV { get; set; }
            public string BookValue_Share { get; set; }
            public string Dvd_Yield { get; set; }
            public string LastUpdate { get; set; }
        }
        private class IAAConsensus
        {

            public IAAConsensus() { }

            // Properties.
            public string Symbol { get; set; }
            public string Broker { get; set; }
            public string EPS_Year { get; set; }
            public string EPS_Year_Change { get; set; }
            public string EPS_2Year { get; set; }
            public string EPS_2Year_Change { get; set; }
            public string PE { get; set; }
            public string PBV { get; set; }
            public string DIV { get; set; }
            public string TargetPrice { get; set; }
            public string Rec { get; set; }
            public string LastUpdate { get; set; }
        }
        private class IAAConsensusSummary
        {

            public IAAConsensusSummary() { }

            // Properties.
            public string Symbol { get; set; }
            public string LastPrice { get; set; }
            public string Buy { get; set; }
            public string Hold { get; set; }
            public string Sell { get; set; }
            public string Average_EPS_Year { get; set; }
            public string Average_EPS_Year_Change { get; set; }
            public string Average_EPS_2Year { get; set; }
            public string Average_EPS_2Year_Change { get; set; }
            public string Average_PE { get; set; }
            public string Average_PBV { get; set; }
            public string Average_DIV { get; set; }
            public string Average_TargetPrice { get; set; }
            public string High_EPS_Year { get; set; }
            public string High_EPS_Year_Change { get; set; }
            public string High_EPS_2Year { get; set; }
            public string High_EPS_2Year_Change { get; set; }
            public string High_PE { get; set; }
            public string High_PBV { get; set; }
            public string High_DIV { get; set; }
            public string High_TargetPrice { get; set; }
            public string Low_EPS_Year { get; set; }
            public string Low_EPS_Year_Change { get; set; }
            public string Low_EPS_2Year { get; set; }
            public string Low_EPS_2Year_Change { get; set; }
            public string Low_PE { get; set; }
            public string Low_PBV { get; set; }
            public string Low_DIV { get; set; }
            public string Low_TargetPrice { get; set; }
            public string Median_EPS_Year { get; set; }
            public string Median_EPS_Year_Change { get; set; }
            public string Median_EPS_2Year { get; set; }
            public string Median_EPS_2Year_Change { get; set; }
            public string Median_PE { get; set; }
            public string Median_PBV { get; set; }
            public string Median_DIV { get; set; }
            public string Median_TargetPrice { get; set; }
            public string LastUpdate { get; set; }
        }
        private class GrowthStock
        {

            public GrowthStock() { }

            // Properties.
            public string Symbol { get; set; }
            public string Net_rate { get; set; }
            public string Assets_rate { get; set; }
            public string Price_rate { get; set; }
            public string LastUpdate { get; set; }
        }
        private class StockDividend
        {

            public StockDividend() { }

            // Properties.
            public string Symbol { get; set; }
            public string DIv_rate { get; set; }
            public string More_one_rate { get; set; }
            public string Double_rate { get; set; }
            public string LastUpdate { get; set; }
        }
        struct GS
        {
            public string Year;
            public double Assets;
            public double Netprofit;
            public double Lastprice;
        }
        struct SD
        {
            public string Year;
            public double Dvd_Yield;
        }
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // | Main Function                                                   |
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        public void Run()
        {
            log.LOGI("Start FindStock");
            List<string> symbols = new List<string>();

            // Get all symbol
            Symbol(ref symbols, "AGRI");
            Symbol(ref symbols, "FOOD");
            Symbol(ref symbols, "FASHION");
            Symbol(ref symbols, "HOME");
            Symbol(ref symbols, "PERSON");
            Symbol(ref symbols, "BANK");
            Symbol(ref symbols, "FIN");
            Symbol(ref symbols, "INSUR");
            Symbol(ref symbols, "AUTO");
            Symbol(ref symbols, "IMM");
            Symbol(ref symbols, "PAPER");
            Symbol(ref symbols, "PETRO");
            Symbol(ref symbols, "PKG");
            Symbol(ref symbols, "STEEL");
            Symbol(ref symbols, "CONMAT");
            Symbol(ref symbols, "PROP");
            Symbol(ref symbols, "PF%26REIT");
            Symbol(ref symbols, "CONS");
            Symbol(ref symbols, "ENERG");
            Symbol(ref symbols, "MINE");
            Symbol(ref symbols, "COMM");
            Symbol(ref symbols, "HELTH");
            Symbol(ref symbols, "MEDIA");
            Symbol(ref symbols, "PROF");
            Symbol(ref symbols, "TOURISM");
            Symbol(ref symbols, "TRANS");
            Symbol(ref symbols, "ETRON");
            Symbol(ref symbols, "ICT");
            Symbol(ref set50, "SET50");
            Symbol(ref set100, "SET100");

            for (var i = 0; i < symbols.Count; i++)
            {
                log.LOGI($"Run {i} {symbols[i]}");
                IAAConsensusScraping(symbols[i]);
                CompanyHighlights(symbols[i]);
                StockScraping(symbols[i]);
            }
            log.LOGI(" Success update data FindStock");
        }
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // | ScrapingWeb Function                                            |
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        private static void Symbol(ref List<string> symbols, string sector)
        {
            var url1 = $"https://marketdata.set.or.th/mkt/sectorquotation.do?sector={sector}&language=th&country=TH";

            // Using HtmlAgilityPack
            var Webget1 = new HtmlWeb();
            var doc1 = Webget1.Load(url1);

            try
            {
                foreach (HtmlNode node in doc1.DocumentNode.SelectNodes("//td//a"))
                {
                    string utf8_String = node.InnerText;
                    byte[] bytes = Encoding.UTF8.GetBytes(utf8_String);
                    utf8_String = Encoding.UTF8.GetString(bytes);
                    utf8_String = utf8_String.Replace("  ", String.Empty);
                    if (utf8_String.IndexOf("\n") >= 0)
                    {
                        utf8_String = utf8_String.Substring(2, utf8_String.Length - 4);
                        symbols.Add(utf8_String);
                    }
                }
            }
            catch
            {
                log.LOGE($"Not scraping data from {url1}");
            }
        }
        private static void CompanyHighlights(string symbol)
        {
            string symbol_url = symbol;
            if (symbol.IndexOf(" & ") > -1)
                symbol_url = symbol.Replace(" & ", "+%26+");
            else if (symbol.IndexOf("&") > -1)
                symbol_url = symbol.Replace("&", "%26");

            var url = $"https://www.set.or.th/set/companyhighlight.do?symbol={symbol_url}&ssoPageId=5";

            // List cut string finance info
            List<string> date_info = new List<string>();
            List<string> year_info = new List<string>();
            List<string> quarter = new List<string>();
            List<string> asset = new List<string>();
            List<string> liabilities = new List<string>();
            List<string> equity = new List<string>();
            List<string> paid_up_cap = new List<string>();
            List<string> revenue = new List<string>();
            List<string> net_profit = new List<string>();
            List<string> eps = new List<string>();
            List<string> roa = new List<string>();
            List<string> roe = new List<string>();
            List<string> net_profit_margin = new List<string>();

            // List cut string finance stat
            List<string> date_stat = new List<string>();
            List<string> year_stat = new List<string>();
            List<string> lastprice = new List<string>();
            List<string> market_cap = new List<string>();
            List<string> fs_date = new List<string>();
            List<string> pe = new List<string>();
            List<string> pbv = new List<string>();
            List<string> book_value_share = new List<string>();
            List<string> dvd_yield = new List<string>();

            // Using HtmlAgilityPack
            var Webget = new HtmlWeb();
            var doc = Webget.Load(url);

            // tmp variable
            var run = "start";

            try
            {
                foreach (HtmlNode node in doc.DocumentNode.SelectNodes("//tr//td"))
                {
                    string utf8_String = node.ChildNodes[0].InnerHtml;
                    byte[] bytes = Encoding.UTF8.GetBytes(utf8_String);
                    utf8_String = Encoding.UTF8.GetString(bytes);
                    int index = utf8_String.IndexOf("&");

                    if (index > 0)
                        utf8_String = utf8_String.Substring(0, utf8_String.IndexOf("&"));

                    if (utf8_String == "N/A" || utf8_String == "N.A." || utf8_String == "-")
                        utf8_String = "null";

                    if (utf8_String == "สินทรัพย์รวม")
                        run = utf8_String;
                    else if (utf8_String == "หนี้สินรวม")
                        run = utf8_String;
                    else if (utf8_String == "ส่วนของผู้ถือหุ้น")
                        run = utf8_String;
                    else if (utf8_String == "มูลค่าหุ้นที่เรียกชำระแล้ว")
                        run = utf8_String;
                    else if (utf8_String == "รายได้รวม")
                        run = utf8_String;
                    else if (utf8_String == "กำไรสุทธิ")
                        run = utf8_String;
                    else if (utf8_String == "กำไรต่อหุ้น (บาท)")
                        run = utf8_String;
                    else if (utf8_String == "ROA(%)")
                        run = utf8_String;
                    else if (utf8_String == "ROE(%)")
                        run = utf8_String;
                    else if (utf8_String == "อัตรากำไรสุทธิ(%)")
                        run = utf8_String;
                    else if (utf8_String == "ราคาล่าสุด(บาท)")
                        run = utf8_String;
                    else if (utf8_String == "มูลค่าหลักทรัพย์ตามราคาตลาด")
                        run = utf8_String;
                    else if (utf8_String == "วันที่ของงบการเงินที่ใช้คำนวณค่าสถิติ")
                        run = utf8_String;
                    else if (utf8_String == "P/E (เท่า)")
                        run = utf8_String;
                    else if (utf8_String == "P/BV (เท่า)")
                        run = utf8_String;
                    else if (utf8_String == "มูลค่าหุ้นทางบัญชีต่อหุ้น (บาท)")
                        run = utf8_String;
                    else if (utf8_String == "อัตราส่วนเงินปันผลตอบแทน(%)")
                        run = utf8_String;

                    if (utf8_String != run)
                        if (run == "สินทรัพย์รวม")
                            asset.Add(utf8_String);
                        else if (run == "หนี้สินรวม")
                            liabilities.Add(utf8_String);
                        else if (run == "ส่วนของผู้ถือหุ้น")
                            equity.Add(utf8_String);
                        else if (run == "มูลค่าหุ้นที่เรียกชำระแล้ว")
                            paid_up_cap.Add(utf8_String);
                        else if (run == "รายได้รวม")
                            revenue.Add(utf8_String);
                        else if (run == "กำไรสุทธิ")
                            net_profit.Add(utf8_String);
                        else if (run == "กำไรต่อหุ้น (บาท)")
                            eps.Add(utf8_String);
                        else if (run == "ROA(%)")
                            roa.Add(utf8_String);
                        else if (run == "ROE(%)")
                            roe.Add(utf8_String);
                        else if (run == "อัตรากำไรสุทธิ(%)")
                            net_profit_margin.Add(utf8_String);
                        else if (run == "ราคาล่าสุด(บาท)")
                            lastprice.Add(utf8_String);
                        else if (run == "มูลค่าหลักทรัพย์ตามราคาตลาด")
                            market_cap.Add(utf8_String);
                        else if (run == "วันที่ของงบการเงินที่ใช้คำนวณค่าสถิติ")
                            fs_date.Add(utf8_String);
                        else if (run == "P/E (เท่า)")
                            pe.Add(utf8_String);
                        else if (run == "P/BV (เท่า)")
                            pbv.Add(utf8_String);
                        else if (run == "มูลค่าหุ้นทางบัญชีต่อหุ้น (บาท)")
                            book_value_share.Add(utf8_String);
                        else if (run == "อัตราส่วนเงินปันผลตอบแทน(%)")
                            dvd_yield.Add(utf8_String);
                }
            }
            catch
            {
                log.LOGE($"Not scraping data from {url}");
            }

            try
            {
                foreach (HtmlNode node in doc.DocumentNode.SelectNodes("//th"))
                {
                    string utf8_String = node.ChildNodes[0].InnerHtml;
                    byte[] bytes = Encoding.UTF8.GetBytes(utf8_String);
                    utf8_String = Encoding.UTF8.GetString(bytes);
                    if (utf8_String == "งวดงบการเงิน<br> ณ วันที่")
                        run = utf8_String;
                    if (utf8_String == "ค่าสถิติสำคัญ<br> ณ วันที่")
                        run = utf8_String;

                    if (utf8_String != run)
                        if (run == "งวดงบการเงิน<br> ณ วันที่" && utf8_String.IndexOf("/") >= 0)
                        {
                            var index = utf8_String.IndexOf(">") + 1;
                            date_info.Add(utf8_String.Substring(index, utf8_String.Length - index));
                            year_info.Add(utf8_String.Substring(utf8_String.Length - 4, 4));
                            quarter.Add(utf8_String.Substring(0, index - 4));
                        }
                        else if (run == "ค่าสถิติสำคัญ<br> ณ วันที่")
                        {
                            date_stat.Add(utf8_String);
                            year_stat.Add(utf8_String.Substring(utf8_String.Length - 4, 4));
                        }

                }
            }
            catch
            {
                log.LOGE($"Not scraping data from {url}");
            }

            List<FinanceInfo> finance_info_yearly = new List<FinanceInfo>();
            List<FinanceInfo> finance_info_quarter = new List<FinanceInfo>();
            List<FinanceStat> finance_stat_yearly = new List<FinanceStat>();
            List<FinanceStat> finance_stat_daily = new List<FinanceStat>();

            for (int i = 0; i < date_stat.Count; i++)
            {
                string fsDate = "";
                if (fs_date[i].IndexOf("/") >= 0)
                    fsDate = fs_date[i];
                else
                    fsDate = null;

                var tmp1 = new FinanceStat
                {
                    Date = (i >= date_stat.Count) ? null : ChangeDateFormat(date_stat[i]),
                    Symbol = symbol,
                    Year = (i >= year_stat.Count) ? null : ChangeYearFormat(year_stat[i]),
                    Lastprice = (i >= lastprice.Count) ? null : CutStrignMoney(lastprice[i]),
                    Market_cap = (i >= market_cap.Count) ? null : CutStrignMoney(market_cap[i]),
                    FS_date = ChangeDateFormat(fsDate),
                    PE = (i >= pe.Count) ? null : CutStrignMoney(pe[i]),
                    PBV = (i >= pbv.Count) ? null : CutStrignMoney(pbv[i]),
                    BookValue_Share = (i >= book_value_share.Count) ? null : CutStrignMoney(book_value_share[i]),
                    Dvd_Yield = (i >= dvd_yield.Count) ? null : CutStrignMoney(dvd_yield[i]),
                    LastUpdate = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                };

                if (i < date_stat.Count - 1)
                {
                    finance_stat_yearly.Add(tmp1);
                }
                else
                {
                    finance_stat_daily.Add(tmp1);
                }
            }

            for (int i = 0; i < date_info.Count; i++)
            {
                var tmp = new FinanceInfo
                {
                    Date = (i >= date_info.Count) ? null : ChangeDateFormat(date_info[i]),
                    Symbol = symbol,
                    Year = (i >= year_info.Count) ? null : ChangeYearFormat(year_info[i]),
                    Quarter = (i >= quarter.Count) ? null : quarter[i],
                    Assets = (i >= asset.Count) ? null : CutStrignMoney(asset[i]),
                    Liabilities = (i >= liabilities.Count) ? null : CutStrignMoney(liabilities[i]),
                    Equity = (i >= equity.Count) ? null : CutStrignMoney(equity[i]),
                    Paid_up_cap = (i >= paid_up_cap.Count) ? null : CutStrignMoney(paid_up_cap[i]),
                    Revenue = (i >= revenue.Count) ? null : CutStrignMoney(revenue[i]),
                    NetProfit = (i >= net_profit.Count) ? null : CutStrignMoney(net_profit[i]),
                    EPS = (i >= eps.Count) ? null : CutStrignMoney(eps[i]),
                    ROA = (i >= roa.Count) ? null : CutStrignMoney(roa[i]),
                    ROE = (i >= roe.Count) ? null : CutStrignMoney(roe[i]),
                    NetProfitMargin = (i >= net_profit_margin.Count) ? null : CutStrignMoney(net_profit_margin[i]),
                    LastUpdate = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                };
                if (tmp.Quarter.IndexOf("ไตรมาส") >= 0)
                {
                    tmp.Quarter = quarter[i].Substring(quarter[i].IndexOf("ไตรมาส") + 6, 1);
                    finance_info_quarter.Add(tmp);
                }
                else
                {
                    tmp.Quarter = null;
                    finance_info_yearly.Add(tmp);
                }
            }

            foreach (var value in finance_info_yearly)
            {
                // Insert or Update datebase finance_info_yearly
                StatementDatabase(value, "finance_info_yearly", $"Date='{value.Date}' AND Symbol='{value.Symbol}'");
            }

            foreach (var value in finance_info_quarter)
            {
                // Insert or Update datebase finance_info_quarter
                StatementDatabase(value, "finance_info_quarter", $"Date='{value.Date}' AND Symbol='{value.Symbol}'");
            }

            foreach (var value in finance_stat_yearly)
            {
                // Insert or Update datebase finance_stat_yearly
                StatementDatabase(value, "finance_stat_yearly", $"Date='{value.Date}' AND Symbol='{value.Symbol}'");
            }

            foreach (var value in finance_stat_daily)
            {
                // Insert or Update datebase finance_stat_daily
                StatementDatabase(value, "finance_stat_daily", $"Date='{value.Date}' AND Symbol='{value.Symbol}'");
            }

            GC.Collect();
            log.LOGI($"Success {symbol}");
        }
        private static void IAAConsensusScraping(string symbol)
        {
            string symbol_url = symbol;
            if (symbol.IndexOf(" & ") > -1)
                symbol_url = symbol.Replace(" & ", "+%26+");
            else if (symbol.IndexOf("&") > -1)
                symbol_url = symbol.Replace("&", "%26");

            var url = $"https://www.settrade.com/AnalystConsensus/C04_10_stock_saa_p1.jsp?txtSymbol={symbol_url}&ssoPageId=11&selectPage=10";
            // Using HtmlAgilityPack
            var Webget1 = new HtmlWeb();
            var doc1 = Webget1.Load(url);

            IAAConsensusSummary iaa_consensus_summary = new IAAConsensusSummary();

            try
            {
                foreach (HtmlNode node in doc1.DocumentNode.SelectNodes("//div[@class='round-border']"))
                {
                    iaa_consensus_summary.Symbol = symbol;
                    iaa_consensus_summary.LastPrice = CutStrignMoney(node.SelectSingleNode(".//div[@class='col-xs-8']//div[@class='text-right']//h1").InnerText);
                    string result = "";
                    foreach (HtmlNode row in node.SelectNodes(".//div[@class='row separate-content']//div[@class='row']"))
                    {
                        foreach (HtmlNode cell in row.SelectNodes("div"))
                            result += $"{cell.InnerText.Replace("\r\n", "").Replace("\n", "").Replace("\r", "")} ";
                    }
                    var parts = result.Split(' ');
                    iaa_consensus_summary.Buy = parts[1];
                    iaa_consensus_summary.Hold = parts[4];
                    iaa_consensus_summary.Sell = parts[7];
                    iaa_consensus_summary.LastUpdate = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                }
            }
            catch
            {
                log.LOGE($"Not scraping data from {url}");
            }

            List<IAAConsensus> iaa_consensus = new List<IAAConsensus>();
            var index = 1;

            try
            {
                foreach (HtmlNode row in doc1.DocumentNode.SelectNodes("//tr"))
                {
                    string result = "";
                    foreach (HtmlNode cell in row.SelectNodes("th|td"))
                    {
                        result += $"{cell.InnerText.Replace("\r\n", "").Replace("\n", "").Replace("\r", "").Replace(" ", "")} ";
                    }
                    var parts = result.Split(' ');
                    if (index.ToString() == parts[0])
                    {
                        var tmp = new IAAConsensus();
                        tmp.Symbol = symbol;
                        tmp.Broker = parts[1];
                        tmp.EPS_Year = parts[2] == "-" ? null : CutStrignMoney(parts[2]);
                        tmp.EPS_Year_Change = parts[3] == "-" ? null : CutStrignMoney(parts[3]);
                        tmp.EPS_2Year = parts[4] == "-" ? null : CutStrignMoney(parts[4]);
                        tmp.EPS_2Year_Change = parts[5] == "-" ? null : CutStrignMoney(parts[5]);
                        tmp.PE = parts[6] == "-" ? null : CutStrignMoney(parts[6]);
                        tmp.PBV = parts[7] == "-" ? null : CutStrignMoney(parts[7]);
                        tmp.DIV = parts[8] == "-" ? null : CutStrignMoney(parts[8]);
                        tmp.TargetPrice = parts[9] == "-" ? null : CutStrignMoney(parts[9]);
                        tmp.Rec = parts[10];
                        tmp.LastUpdate = ChangeDateFormat2(parts[11]);
                        index++;
                        iaa_consensus.Add(tmp);
                    }
                    else if (parts[0] == "Average")
                    {
                        iaa_consensus_summary.Average_EPS_Year = parts[1] == "-" ? null : CutStrignMoney(parts[1]);
                        iaa_consensus_summary.Average_EPS_Year_Change = parts[2] == "-" ? null : CutStrignMoney(parts[2]);
                        iaa_consensus_summary.Average_EPS_2Year = parts[3] == "-" ? null : CutStrignMoney(parts[3]);
                        iaa_consensus_summary.Average_EPS_2Year_Change = parts[4] == "-" ? null : CutStrignMoney(parts[4]);
                        iaa_consensus_summary.Average_PE = parts[5] == "-" ? null : CutStrignMoney(parts[5]);
                        iaa_consensus_summary.Average_PBV = parts[6] == "-" ? null : CutStrignMoney(parts[6]);
                        iaa_consensus_summary.Average_DIV = parts[7] == "-" ? null : CutStrignMoney(parts[7]);
                        iaa_consensus_summary.Average_TargetPrice = parts[8] == "-" ? null : CutStrignMoney(parts[8]);
                    }
                    else if (parts[0] == "High")
                    {
                        iaa_consensus_summary.High_EPS_Year = parts[1] == "-" ? null : CutStrignMoney(parts[1]);
                        iaa_consensus_summary.High_EPS_Year_Change = parts[2] == "-" ? null : CutStrignMoney(parts[2]);
                        iaa_consensus_summary.High_EPS_2Year = parts[3] == "-" ? null : CutStrignMoney(parts[3]);
                        iaa_consensus_summary.High_EPS_2Year_Change = parts[4] == "-" ? null : CutStrignMoney(parts[4]);
                        iaa_consensus_summary.High_PE = parts[5] == "-" ? null : CutStrignMoney(parts[5]);
                        iaa_consensus_summary.High_PBV = parts[6] == "-" ? null : CutStrignMoney(parts[6]);
                        iaa_consensus_summary.High_DIV = parts[7] == "-" ? null : CutStrignMoney(parts[7]);
                        iaa_consensus_summary.High_TargetPrice = parts[8] == "-" ? null : CutStrignMoney(parts[8]);
                    }
                    else if (parts[0] == "Low")
                    {
                        iaa_consensus_summary.Low_EPS_Year = parts[1] == "-" ? null : CutStrignMoney(parts[1]);
                        iaa_consensus_summary.Low_EPS_Year_Change = parts[2] == "-" ? null : CutStrignMoney(parts[2]);
                        iaa_consensus_summary.Low_EPS_2Year = parts[3] == "-" ? null : CutStrignMoney(parts[3]);
                        iaa_consensus_summary.Low_EPS_2Year_Change = parts[4] == "-" ? null : CutStrignMoney(parts[4]);
                        iaa_consensus_summary.Low_PE = parts[5] == "-" ? null : CutStrignMoney(parts[5]);
                        iaa_consensus_summary.Low_PBV = parts[6] == "-" ? null : CutStrignMoney(parts[6]);
                        iaa_consensus_summary.Low_DIV = parts[7] == "-" ? null : CutStrignMoney(parts[7]);
                        iaa_consensus_summary.Low_TargetPrice = parts[8] == "-" ? null : CutStrignMoney(parts[8]);
                    }
                    else if (parts[0] == "Median")
                    {
                        iaa_consensus_summary.Median_EPS_Year = parts[1] == "-" ? null : CutStrignMoney(parts[1]);
                        iaa_consensus_summary.Median_EPS_Year_Change = parts[2] == "-" ? null : CutStrignMoney(parts[2]);
                        iaa_consensus_summary.Median_EPS_2Year = parts[3] == "-" ? null : CutStrignMoney(parts[3]);
                        iaa_consensus_summary.Median_EPS_2Year_Change = parts[4] == "-" ? null : CutStrignMoney(parts[4]);
                        iaa_consensus_summary.Median_PE = parts[5] == "-" ? null : CutStrignMoney(parts[5]);
                        iaa_consensus_summary.Median_PBV = parts[6] == "-" ? null : CutStrignMoney(parts[6]);
                        iaa_consensus_summary.Median_DIV = parts[7] == "-" ? null : CutStrignMoney(parts[7]);
                        iaa_consensus_summary.Median_TargetPrice = parts[8] == "-" ? null : CutStrignMoney(parts[8]);
                    }

                }
            }
            catch
            {
                log.LOGE($"Not scraping data from {url}");
            }

            foreach (var value in iaa_consensus)
            {
                // Insert or Update datebase iaa_consensus
                StatementDatabase(value, "iaa_consensus", $"Symbol='{value.Symbol}' AND Broker='{value.Broker}'");
            }

            // Insert or Update datebase iaa_consensus_summary
            StatementDatabase(iaa_consensus_summary, "iaa_consensus_summary", $"Symbol='{iaa_consensus_summary.Symbol}'");


            GC.Collect();
            log.LOGI($"Success {symbol}");
        }
        private static void StockScraping(string symbol)
        {
            string symbol_url = symbol;
            if (symbol.IndexOf(" & ") > -1)
                symbol_url = symbol.Replace(" & ", "+%26+");
            else if (symbol.IndexOf("&") > -1)
                symbol_url = symbol.Replace("&", "%26");

            var url = $"https://www.set.or.th/set/companyprofile.do?symbol={symbol_url}&ssoPageId=4&language=th&country=TH";
            // Using HtmlAgilityPack
            var Webget = new HtmlWeb();
            var doc = Webget.Load(url);

            Stock stock = new Stock();
            stock.Symbol = symbol;
            stock.LastUpdate = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            foreach (var value in set50)
                if (value == symbol)
                {
                    stock.SET50 = "1";
                    stock.SET50_rate = "100";
                    break;
                }
                else
                {
                    stock.SET50 = "0";
                    stock.SET50_rate = "0";
                }
            foreach (var value in set100)
                if (value == symbol)
                {
                    stock.SET100 = "1";
                    stock.SET100_rate = "100";
                    break;
                }
                else
                {
                    stock.SET100 = "0";
                    stock.SET100_rate = "0";
                }
            try
            {
                string name = doc.DocumentNode.SelectSingleNode("//div[@class='row']//h3").InnerText;
                stock.Name = name.Substring(name.IndexOf(":") + 2, name.Length - name.IndexOf(":") - 2);
            }
            catch
            {
                log.LOGE($"Not scraping data from {url}");
            }

            try
            {
                foreach (HtmlNode node in doc.DocumentNode.SelectNodes("//td//div[@class='row']//div[@class='row']//div[@class='col-xs-3 col-md-7']"))
                {
                    string head = node.SelectSingleNode("..//div[@class='col-xs-3 col-md-7']").InnerText;
                    string body = node.SelectSingleNode("..//div[@class='col-xs-9 col-md-5']").InnerText;
                    if (head == "ตลาด")
                        stock.Market = body;
                    else if (head == "กลุ่มอุตสาหกรรม")
                        stock.Industry = body;
                    else if (head == "หมวดธุรกิจ")
                        stock.Sector = body;
                    else if (head == "วันที่เริ่มต้นซื้อขาย")
                        stock.First_trade_date = ChangeDateFormat3(body);
                }
            }
            catch
            {
                log.LOGE($"Not scraping data from {url}");
            }

            try
            {
                foreach (HtmlNode node in doc.DocumentNode.SelectNodes("//td//div[@class='row']//div[@class='col-xs-12 col-md-3']"))
                {
                    string head = node.SelectSingleNode("..//div[@class='col-xs-12 col-md-3']").InnerText;
                    string body = node.SelectSingleNode("..//div[@class='col-xs-12 col-md-9']").InnerText;
                    if (head == "เว็บไซต์")
                        stock.Website = body;
                }
            }
            catch
            {
                log.LOGE($"Not scraping data from {url}");
            }

            try
            {
                foreach (HtmlNode node in doc.DocumentNode.SelectNodes("//td//div[@class='row']//div[@class='col-xs-12 col-md-8']"))
                {
                    string head = node.SelectSingleNode("..//div[@class='col-xs-12 col-md-8']//div[@class='col-xs-3 col-md-7']").InnerText;
                    string body = node.SelectSingleNode("..//div[@class='col-xs-12 col-md-8']//div[@class='col-xs-9 col-md-5']").InnerText;
                    head = head.Replace(" ", String.Empty);
                    body = body.Replace(" ", String.Empty);
                    if (head == "มูลค่าหลักทรัพย์ตามราคาตลาด(ล้านบาท)")
                        stock.Market_cap = body != "-" ? CutStrignMoney(body) : null;
                }
            }
            catch
            {
                log.LOGE($"Not scraping data from {url}");
            }

            try
            {
                url = $"https://www.finnomena.com/stock/{symbol_url}";
                var Webget1 = new HtmlWeb();
                var doc1 = Webget.Load(url);
                foreach (HtmlNode node in doc1.DocumentNode.SelectNodes("//div[@class='performance-a-year']//p"))
                {
                    string utf8_String = node.InnerText;
                    byte[] bytes = Encoding.UTF8.GetBytes(utf8_String);
                    utf8_String = Encoding.UTF8.GetString(bytes);
                    utf8_String = utf8_String.Replace("  ", String.Empty);
                    if (utf8_String.IndexOf("%") > -1)
                        utf8_String = utf8_String.Substring(1, utf8_String.IndexOf("%") - 2);
                    if (utf8_String == "N/A")
                        utf8_String = null;
                    stock.Return_rate = utf8_String;
                }
            }
            catch
            {
                log.LOGE($"Not scraping data from {url}");
            }

            IAAPersent(ref stock, "iaa_consensus_summary", $"Symbol = '{stock.Symbol}'");
            GrowthStockPersent(ref stock);
            StockDividendPersent(ref stock);
            FinancialStat(ref stock);
            FinancialInfo(ref stock);

            // cal score
            double sum = 136;
            double[] score = new double[16];
            for (int i = 0; i < 16; i++)
            {
                score[i] = ((i + 1) * 100.00 / sum);
            }

            double score_result = 0;
            var x = score_result = 0;
            
            score_result += stock.Return_rate == null ? 0 : Convert.ToDouble(stock.Return_rate) >= 0 ? (score[0] * 100.00 / 100.00) : 0;
            score_result += stock.Price_rate == null ? 0 : Convert.ToDouble(stock.Price_rate) >= 0 ? (score[1] * 100.00 / 100.00) : 0;
            score_result += stock.IAA_rate == null ? 0 : score[2] * Convert.ToDouble(stock.IAA_rate) / 100.00;
            score_result += stock.Growth_stock_rate == null ? 0 : (score[15] + score[4] + score[14]) * Convert.ToDouble(stock.Growth_stock_rate) / 100.00;
            score_result += stock.Stock_dividend_rate == null ? 0 : (score[13] + score[11] + score[12]) * Convert.ToDouble(stock.Stock_dividend_rate) / 100.00;
            score_result += stock.SET50_rate == null ? 0 : score[10] * Convert.ToDouble(stock.SET50_rate) / 100.00;
            score_result += stock.SET100_rate == null ? 0 : score[7] * Convert.ToDouble(stock.SET100_rate) / 100.00;
            score_result += stock.PE_rate == null ? 0 : score[6] * Convert.ToDouble(stock.PE_rate) / 100.00;
            score_result += stock.PBV_rate == null ? 0 : score[5] * Convert.ToDouble(stock.PBV_rate) / 100.00;
            score_result += stock.ROE_rate == null ? 0 : score[8] * Convert.ToDouble(stock.ROE_rate) / 100.00;
            score_result += stock.ROA_rate == null ? 0 : score[9] * Convert.ToDouble(stock.ROA_rate) / 100.00;
            score_result += stock.Market_cap_rate == null ? 0 : score[3] * Convert.ToDouble(stock.Market_cap_rate) / 100.00;

            stock.Score = score_result.ToString();

            // Insert or Update datebase stock
            StatementDatabase(stock, "stock", $"Symbol='{stock.Symbol}'");
            log.LOGI($"Success {symbol}");
        }
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // | Database Function                                               |
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        private static string MCavr(ref Stock stock, double mc)
        {
            string sql = "";
            string connetionString;

            connetionString = $"Persist Security Info=False;server={DatabaseServer};database={Database};uid={Username};password={Password}";
            MySqlConnection cnn = new MySqlConnection(connetionString);
            MySqlCommand command = cnn.CreateCommand();

            sql = $"SELECT stock.Symbol " +
                $",finance_stat_daily.Market_cap " +
                $"FROM stock " +
                $"INNER JOIN finance_stat_daily ON stock.Symbol = finance_stat_daily.Symbol " +
                $"WHERE finance_stat_daily.LastUpdate IN (SELECT max(finance_stat_daily.LastUpdate) FROM finance_stat_daily) " +
                $"AND Sector = '{stock.Sector}'";

            command.CommandText = sql;

            try
            {
                cnn.Open();
            }
            catch (Exception erro)
            {
                log.LOGE("Erro" + erro);
            }

            double mc_result = 0, count = 0;
            using (MySqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    mc_result += Convert.ToDouble(String.Format("{0}", reader["Market_cap"]));
                    count++;
                }
            }

            cnn.Close();

            return (mc > (mc_result / count)) ? "100" : "0";
        }
        private static string PEavr(ref Stock stock, double pe)
        {
            string sql = "";
            string connetionString;
            connetionString = $"Persist Security Info=False;server={DatabaseServer};database={Database};uid={Username};password={Password}";
            MySqlConnection cnn = new MySqlConnection(connetionString);
            MySqlCommand command = cnn.CreateCommand();

            sql = $"SELECT stock.Symbol " +
                $",finance_stat_daily.PE " +
                $"FROM stock " +
                $"INNER JOIN finance_stat_daily ON stock.Symbol = finance_stat_daily.Symbol " +
                $"WHERE finance_stat_daily.LastUpdate IN (SELECT max(finance_stat_daily.LastUpdate) FROM finance_stat_daily) " +
                $"AND Sector = '{stock.Sector}'";

            command.CommandText = sql;

            try
            {
                cnn.Open();
            }
            catch (Exception erro)
            {
                log.LOGE("Erro" + erro);
            }

            double pe_result = 0, count = 0;

            using (MySqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    pe_result += Convert.ToDouble(String.Format("{0}", reader["PE"]));
                    count++;
                }
            }

            cnn.Close();

            return (pe <= (pe_result / count)) ? "25" : "0";
        }
        private static void FinancialInfo(ref Stock stock)
        {
            string sql = "";
            string connetionString;

            connetionString = $"Persist Security Info=False;server={DatabaseServer};database={Database};uid={Username};password={Password}";
            MySqlConnection cnn = new MySqlConnection(connetionString);
            MySqlCommand command = cnn.CreateCommand();

            sql = $"SELECT Symbol " +
                $",ROA " +
                $",ROE " +
                $"FROM {Database}.finance_info_yearly " +
                $"WHERE Symbol = '{stock.Symbol}' " +
                $"AND Date IN (SELECT max(Date) FROM {Database}.finance_info_yearly WHERE Symbol = '{stock.Symbol}') LIMIT 1 ";

            command.CommandText = sql;

            try
            {
                cnn.Open();
            }
            catch (Exception erro)
            {
                log.LOGE("Erro" + erro);
            }

            using (MySqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    double roa = Convert.ToDouble(String.Format("{0}", reader["ROA"]) != "" ? String.Format("{0}", reader["ROA"]) : "0");
                    double roe = Convert.ToDouble(String.Format("{0}", reader["ROE"]) != "" ? String.Format("{0}", reader["ROE"]) : "0");
                    stock.ROA_rate = (roa >= 15) ? "100" : (roa >= 10 && roa < 15) ? "50" : "0";
                    stock.ROE_rate = (roe >= 15) ? "100" : "0";
                }
            }

            cnn.Close();
        }
        private static void FinancialStat(ref Stock stock)
        {
            string sql = "";
            string connetionString;

            connetionString = $"Persist Security Info=False;server={DatabaseServer};database={Database};uid={Username};password={Password}";
            MySqlConnection cnn = new MySqlConnection(connetionString);
            MySqlCommand command = cnn.CreateCommand();

            sql = $"SELECT Symbol " +
                $",PE " +
                $",PBV " +
                $",Market_cap " +
                $"FROM {Database}.finance_stat_daily " +
                $"WHERE Symbol = '{stock.Symbol}' " +
                $"AND LastUpdate IN (SELECT max(LastUpdate) FROM {Database}.finance_stat_daily WHERE Symbol = '{stock.Symbol}') LIMIT 1 ";

            command.CommandText = sql;

            try
            {
                cnn.Open();
            }
            catch (Exception erro)
            {
                log.LOGE("Erro" + erro);
            }

            using (MySqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    double pe = Convert.ToDouble(String.Format("{0}", reader["PE"]) != "" ? String.Format("{0}", reader["PE"]) : "0");
                    double pbv = Convert.ToDouble(String.Format("{0}", reader["PBV"]) != "" ? String.Format("{0}", reader["PBV"]) : "0");
                    double mc = Convert.ToDouble(String.Format("{0}", reader["Market_cap"]) != "" ? String.Format("{0}", reader["Market_cap"]) : "0");
                    stock.PE_rate = (pe <= 5) ? "100" : (pe > 5 && pe <= 10) ? "75" : (pe > 10 && pe <= 18) ? "50" : PEavr(ref stock, pe);
                    stock.PBV_rate = (pbv <= 1) ? "100" : "0";
                    stock.Market_cap_rate = MCavr(ref stock, mc);
                }
            }

            cnn.Close();
        }
        private static void StockDividendPersent(ref Stock stock)
        {
            string symbol_url = stock.Symbol;
            if (stock.Symbol.IndexOf(" & ") > -1)
                symbol_url = stock.Symbol.Replace(" & ", "+%26+");
            else if (stock.Symbol.IndexOf("&") > -1)
                symbol_url = stock.Symbol.Replace("&", "%26");

            string sql = "";
            string connetionString;

            connetionString = $"Persist Security Info=False;server={DatabaseServer};database={Database};uid={Username};password={Password}";
            MySqlConnection cnn = new MySqlConnection(connetionString);
            MySqlCommand command = cnn.CreateCommand();

            sql = $"SELECT Symbol,Year,Dvd_Yield FROM {Database}.finance_stat_yearly WHERE Symbol = '{stock.Symbol}'";

            command.CommandText = sql;

            try
            {
                cnn.Open();
            }
            catch (Exception erro)
            {
                log.LOGE("Erro" + erro);
            }

            List<SD> stock_div = new List<SD>();
            StockDividend stock_dividend = new StockDividend();
            stock_dividend.Symbol = stock.Symbol;
            stock_dividend.LastUpdate = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            using (MySqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    SD tmp;
                    tmp.Year = String.Format("{0}", reader["Year"]);
                    tmp.Dvd_Yield = Convert.ToDouble(String.Format("{0}", reader["Dvd_Yield"]));
                    stock_div.Add(tmp);
                }
            }

            stock_div = stock_div.OrderByDescending(o => o.Year).ToList();
            int i = 0;
            List<SD> stock_divt_rate = new List<SD>();
            foreach (var value in stock_div)
            {

                stock_divt_rate.Add(value);
                if (i++ > 5)
                    break;
            }
            stock_divt_rate = stock_divt_rate.OrderBy(o => o.Year).ToList();
            int x = 0, y = 0;
            foreach (var value in stock_divt_rate)
            {
                if (value.Dvd_Yield > 0)
                    x++;
                if (value.Dvd_Yield > 1)
                    y++;
            }

            double unappropriated = 0, dividend_paid = 0;

            // Scraping Web
            var url = $"https://www.set.or.th/set/companyfinance.do?symbol={symbol_url}";
            var Webget = new HtmlWeb();
            var doc = Webget.Load(url);
            try
            {
                foreach (HtmlNode node in doc.DocumentNode.SelectNodes("//div[@class='table-responsive']//tr"))
                {
                    string head = node.SelectSingleNode(".//td[@style='text-align:left;']").InnerText;
                    head = head.Replace("&nbsp;", String.Empty);
                    if (head == "กำไร (ขาดทุน) สะสม - ยังไม่ได้จัดสรร")
                    {
                        string body = node.ChildNodes[5].InnerText;
                        unappropriated = Convert.ToDouble(CutStrignMoney(body));
                    }
                }
            }
            catch
            {
                log.LOGE($"Not scraping data from {url}");
            }


            // Scraping Web
            url = $"https://www.set.or.th/set/companyfinance.do?type=cashflow&symbol={symbol_url}";

            var Webget1 = new HtmlWeb();
            var doc1 = Webget1.Load(url);
            try
            {
                foreach (HtmlNode node in doc1.DocumentNode.SelectNodes("//div[@class='table-responsive']//tr"))
                {
                    string head = node.SelectSingleNode(".//td[@style='text-align:left;']").InnerText;
                    head = head.Replace("&nbsp;", String.Empty);
                    if (head == "เงินปันผลจ่าย")
                    {
                        string body = node.ChildNodes[5].InnerText;
                        body = body.Replace("-", String.Empty);
                        dividend_paid = Convert.ToDouble(CutStrignMoney(body));
                    }
                }
            }
            catch
            {
                log.LOGE($"Not scraping data from {url}");
            }

            double Double_rate = 0, DIv_rate = 0, More_one_rate = 0;
            if (unappropriated > (dividend_paid * 2))
                Double_rate = 100;

            if (i > 0)
            {
                DIv_rate = Math.Round((double)(x * 100 / (double)(i)));
                More_one_rate = Math.Round((double)(y * 100 / (double)(i)));
            }
            stock.Stock_dividend_rate = (((DIv_rate / 100 * 14.00) + (More_one_rate / 100 * 12.00) + (Double_rate / 100 * 13.00)) * 100.00 / 39).ToString();
            stock_dividend.DIv_rate = DIv_rate.ToString();
            stock_dividend.More_one_rate = More_one_rate.ToString();
            stock_dividend.Double_rate = Double_rate.ToString();

            StatementDatabase(stock_dividend, "stock_dividend", $"Symbol='{stock.Symbol}'");

            cnn.Close();
        }
        private static void GrowthStockPersent(ref Stock stock)
        {
            string sql = "";
            string connetionString;
            connetionString = $"Persist Security Info=False;server={DatabaseServer};database={Database};uid={Username};password={Password}";
            MySqlConnection cnn = new MySqlConnection(connetionString);
            MySqlCommand command = cnn.CreateCommand();

            sql = $"SELECT {Database}.finance_info_yearly.Symbol, " +
                $"{Database}.finance_info_yearly.Year, " +
                $"{Database}.finance_info_yearly.Assets, " +
                $"{Database}.finance_info_yearly.NetProfit, " +
                $"{Database}.finance_stat_yearly.Lastprice " +
                $"FROM {Database}.finance_info_yearly " +
                $"INNER JOIN {Database}.finance_stat_yearly " +
                $"ON {Database}.finance_info_yearly.Symbol = {Database}.finance_stat_yearly.Symbol " +
                $"AND {Database}.finance_info_yearly.Year = {Database}.finance_stat_yearly.Year " +
                $"AND {Database}.finance_info_yearly.Symbol = '{stock.Symbol}' ";

            command.CommandText = sql;

            try
            {
                cnn.Open();
            }
            catch (Exception erro)
            {
                log.LOGE("Erro" + erro);
            }

            List<GS> netprofit = new List<GS>();
            GrowthStock growth_stock = new GrowthStock();
            growth_stock.Symbol = stock.Symbol;
            growth_stock.LastUpdate = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            using (MySqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    GS tmp;
                    tmp.Year = String.Format("{0}", reader["Year"]);
                    tmp.Assets = Convert.ToDouble(String.Format("{0}", reader["Assets"]));
                    tmp.Netprofit = Convert.ToDouble(String.Format("{0}", reader["NetProfit"]));
                    tmp.Lastprice = Convert.ToDouble(String.Format("{0}", reader["Lastprice"]));
                    netprofit.Add(tmp);
                }
            }

            // Net growth continues persent
            netprofit = netprofit.OrderByDescending(o => o.Year).ToList();
            int i = 0;
            List<GS> netprofit_rate = new List<GS>();
            foreach (var value in netprofit)
            {

                netprofit_rate.Add(value);
                if (i++ > 9)
                    break;
            }
            netprofit_rate = netprofit_rate.OrderBy(o => o.Year).ToList();
            int x = 0, y = 0, z = 0;
            for (int j = 0; j < i - 1; j++)
            {
                if (netprofit_rate[j + 1].Assets > netprofit_rate[j].Assets)
                    x++;
                if (netprofit_rate[j + 1].Netprofit > netprofit_rate[j].Netprofit)
                    y++;
                if (netprofit_rate[j + 1].Lastprice > netprofit_rate[j].Lastprice)
                    z++;
            }

            if (i > 1)
            {
                double Assets_rate = Math.Round((double)(x * 100 / (double)(i - 1)));
                double Net_rate = Math.Round((double)(y * 100 / (double)(i - 1)));
                double Price_rate = Math.Round((double)(z * 100 / (double)(i - 1)));
                stock.Growth_stock_rate = (((Assets_rate / 100 * 15.00) + (Net_rate / 100 * 16.00) + (Price_rate / 100 * 5.00)) * 100.00 / 36).ToString();
                growth_stock.Assets_rate = Assets_rate.ToString();
                growth_stock.Net_rate = Net_rate.ToString();
                growth_stock.Price_rate = Price_rate.ToString();
            }

            StatementDatabase(growth_stock, "growth_stock", $"Symbol='{stock.Symbol}'");

            cnn.Close();
        }
        private static void IAAPersent(ref Stock stock, string db, string where)
        {
            string sql = "";
            string connetionString;
            connetionString = $"Persist Security Info=False;server={DatabaseServer};database={Database};uid={Username};password={Password}";
            MySqlConnection cnn = new MySqlConnection(connetionString);
            MySqlCommand command = cnn.CreateCommand();

            sql = $"Select * from {Database}.{db} where {where}";

            command.CommandText = sql;

            try
            {
                cnn.Open();
            }
            catch (Exception erro)
            {
                log.LOGE("Erro" + erro);
            }

            using (MySqlDataReader reader = command.ExecuteReader())
            {
                if (reader.Read())
                {
                    if (String.Format("{0}", reader["Average_TargetPrice"]) != "")
                    {
                        double last_price = Convert.ToDouble(String.Format("{0}", reader["LastPrice"]));
                        double target_price = Convert.ToDouble(String.Format("{0}", reader["Average_TargetPrice"]));
                        stock.Price_rate = Math.Round((target_price - last_price) * 100 / last_price, 2).ToString();
                        double buy = Convert.ToInt32(String.Format("{0}", reader["Buy"]));
                        double hold = Convert.ToInt32(String.Format("{0}", reader["Hold"]));
                        double sell = Convert.ToInt32(String.Format("{0}", reader["Sell"]));
                        if ((buy + hold + sell) > 0)
                            stock.IAA_rate = Math.Round((buy + (hold / 2)) * 100 / (buy + hold + sell), 2).ToString();
                    }
                }
            }

            cnn.Close();
        }
        private static void StatementDatabase(object item, string db, string where, bool none_update = false)
        {
            string sql = "";
            string connetionString;
            connetionString = $"Persist Security Info=False;server={DatabaseServer};database={Database};uid={Username};password={Password}";
            MySqlConnection cnn = new MySqlConnection(connetionString);
            MySqlCommand command = cnn.CreateCommand();

            sql = $"Select * from {Database}.{db} where {where}";

            command.CommandText = sql;

            try
            {
                cnn.Open();
            }
            catch (Exception erro)
            {
                log.LOGE("Erro" + erro);
            }

            bool event_case = true;
            using (MySqlDataReader reader = command.ExecuteReader())
            {
                if (reader.Read())
                {
                    sql = GetUpdateSQL(item, db, where);
                    event_case = false;
                }
                else
                {
                    //Insert share_holder_equity
                    sql = GetInsertSQL(item, db);
                }
            }
            if (event_case)
                InsertDatebase(sql, cnn);
            else if (!none_update && !event_case)
                UpdateDatebase(sql, cnn);

            cnn.Close();
        }
        private static void UpdateDatebase(string sql, MySqlConnection cnn)
        {
            MySqlCommand command;
            MySqlDataAdapter adapter = new MySqlDataAdapter();

            try
            {
                command = new MySqlCommand(sql, cnn);
                adapter.UpdateCommand = new MySqlCommand(sql, cnn);
                adapter.UpdateCommand.ExecuteNonQuery();
                command.Dispose();
            }
            catch
            {
                log.LOGE($"[FundamentalSET100::UpdateDatebase]  {sql}");
            }
        }
        private static void InsertDatebase(string sql, MySqlConnection cnn)
        {
            MySqlCommand command;
            MySqlDataAdapter adapter = new MySqlDataAdapter();

            try
            {
                command = new MySqlCommand(sql, cnn);
                adapter.InsertCommand = new MySqlCommand(sql, cnn);
                adapter.InsertCommand.ExecuteNonQuery();
                command.Dispose();
            }
            catch
            {
                log.LOGE($"[FundamentalSET100::InsertDatebase]  {sql}");
            }
        }
        private static string GetInsertSQL(object item, string db)
        {
            string sql = $"INSERT INTO {Database}.{db} (:columns:) VALUES (:values:);";

            string[] columns = new string[item.GetType().GetProperties().Count()];
            string[] values = new string[item.GetType().GetProperties().Count()];
            int i = 0;
            foreach (var propertyInfo in item.GetType().GetProperties())
            {
                columns[i] = propertyInfo.Name;
                values[i++] = (string)(propertyInfo.GetValue(item, null));
            }

            //replacing the markers with the desired column names and values
            sql = FillColumnsAndValuesIntoInsertQuery(sql, columns, values);

            return sql;
        }
        private static string GetUpdateSQL(object item, string db, string whare)
        {
            string sql = $"UPDATE {Database}.{db} SET :update: WHERE {whare} ;";

            string[] columns = new string[item.GetType().GetProperties().Count()];
            string[] values = new string[item.GetType().GetProperties().Count()];
            int i = 0;
            foreach (var propertyInfo in item.GetType().GetProperties())
            {
                columns[i] = propertyInfo.Name;
                values[i++] = (string)(propertyInfo.GetValue(item, null));
            }

            //replacing the markers with the desired column names and values
            sql = FillColumnsAndValuesIntoUpdateQuery(sql, columns, values);

            return sql;
        }
        private static string FillColumnsAndValuesIntoInsertQuery(string query, string[] columns, string[] values)
        {
            //joining the string arrays with a comma character
            string columnnames = ("`" + string.Join("`,`", (columns)) + "`");
            //adding values with single quotation marks around them to handle errors related to string values
            string valuenames = ("'" + string.Join("','", values) + "'").Replace("''", "null");
            //replacing the markers with the desired column names and values
            return query.Replace(":columns:", columnnames).Replace(":values:", valuenames);
        }
        private static string FillColumnsAndValuesIntoUpdateQuery(string query, string[] columns, string[] values)
        {
            string result = "";
            for (int i = 0; i < columns.Length; i++)
                if (values[i] != null)
                    result += $"`{columns[i]}` = '{values[i]}'" + (i + 1 != columns.Length ? ", " : " ");
                else
                    result += $"`{columns[i]}` = null" + (i + 1 != columns.Length ? ", " : " ");
            //replacing the markers with the desired column names and values
            return query.Replace(":update:", result);
        }
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // | Other    Function                                               |
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = 
        private static string CutStrignMoney(string money)
        {
            if (money == "null")
                return "0";
            else
                return money.Replace(",", "").Replace("*", "");
        }
        private static string ChangeDateFormat(string date)
        {
            if (date == null)
                return date;

            string str = date + "/";
            var parts = str.Split('/');
            int dd = Convert.ToInt32(parts[0]);
            int mm = Convert.ToInt32(parts[1]);
            int yy = Convert.ToInt32(parts[2]) - 543;

            return $"{yy}-{mm}-{dd}";
        }
        private static string ChangeDateFormat2(string date)
        {
            if (date == null)
                return date;

            string str = date + "/";
            var parts = str.Split('/');
            int dd = Convert.ToInt32(parts[0]);
            int mm = Convert.ToInt32(parts[1]);
            int yy = Convert.ToInt32(parts[2]) + 2000;

            return $"{yy}-{mm}-{dd}";
        }
        private static string ChangeDateFormat3(string date)
        {
            if (date == "null")
                return date;

            var parts = date.Split(' ');
            int mm;
            switch (parts[1])
            {
                case "ม.ค.":
                    mm = 1;
                    break;
                case "ก.พ.":
                    mm = 2;
                    break;
                case "มี.ค.":
                    mm = 3;
                    break;
                case "เม.ย.":
                    mm = 4;
                    break;
                case "พ.ค.":
                    mm = 5;
                    break;
                case "มิ.ย.":
                    mm = 6;
                    break;
                case "ก.ค.":
                    mm = 7;
                    break;
                case "ส.ค.":
                    mm = 8;
                    break;
                case "ก.ย.":
                    mm = 9;
                    break;
                case "ต.ค.":
                    mm = 10;
                    break;
                case "พ.ย.":
                    mm = 11;
                    break;
                default:
                    mm = 12;
                    break;
            }
            int dd = Convert.ToInt32(parts[0]);
            int yy = Convert.ToInt32(parts[2]) - 543;

            if (parts.Length > 3)
                return $"{yy}-{mm}-{dd} {parts[3]}:00.000";

            else

                return $"{yy}-{mm}-{dd}";
        }
        private static string ChangeYearFormat(string year)
        {
            int yy = Convert.ToInt32(year) - 543;
            return $"{yy}";
        }
    }
}
