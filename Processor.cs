using HtmlAgilityPack;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
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
            public string Market { get; set; }
            public string Industry { get; set; }
            public string Sector { get; set; }
            public string SET50 { get; set; }
            public string SET100 { get; set; }
            public string Return_rate { get; set; }
            public string Price { get; set; }
            public string IAA { get; set; }
            public string Growth_stock { get; set; }
            public string Stock_dividend { get; set; }
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
        struct GS
        {
            public string Year;
            public double Assets;
            public double Netprofit;
            public double Lastprice;
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
            List<IAAConsensus> iaa_consensus = new List<IAAConsensus>();
            var index = 1;
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
                    break;
                }
                else
                    stock.SET50 = "0";
            foreach (var value in set100)
                if (value == symbol)
                {
                    stock.SET100 = (value == symbol) ? "1" : "0";
                    break;
                }
                else
                    stock.SET100 = "0";

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
            }

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

            IAAPersent(ref stock, "iaa_consensus_summary", $"Symbol = '{stock.Symbol}'");
            GrowthStockPersent(ref stock);

            /*log.LOGD($"Symbol: {stock.Symbol}");
            log.LOGD($"Market: {stock.Market}");
            log.LOGD($"Industry: {stock.Industry}");
            log.LOGD($"Sector: {stock.Sector}");
            log.LOGD($"SET50: {stock.SET50}");
            log.LOGD($"SET100: {stock.SET100}");
            log.LOGD($"Return_rate: {stock.Return_rate}");
            log.LOGD($"Price: {stock.Price}");
            log.LOGD($"IAA: {stock.IAA}");
            log.LOGD($"Growth_stock: {stock.Growth_stock}");
            log.LOGD($"Stock_dividend: {stock.Stock_dividend}");
            log.LOGD($"LastUpdate: {stock.LastUpdate}");*/

            // Insert or Update datebase stock
            StatementDatabase(stock, "stock", $"Symbol='{stock.Symbol}'");
            log.LOGI($"Success {symbol}");
        }
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // | Database Function                                               |
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        private static void GrowthStockPersent(ref Stock stock)
        {
            string sql = "";
            string connetionString;
            SqlConnection cnn;
            connetionString = $@"Data Source={DatabaseServer};Initial Catalog={Database};User ID={Username};Password={Password}";
            cnn = new SqlConnection(connetionString);
            cnn.Open();

            sql = $"SELECT {Database}.dbo.finance_info_yearly.Symbol, " +
                $"{Database}.dbo.finance_info_yearly.Year, " +
                $"{Database}.dbo.finance_info_yearly.Assets, " +
                $"{Database}.dbo.finance_info_yearly.NetProfit, " +
                $"{Database}.dbo.finance_stat_yearly.Lastprice " +
                $"FROM {Database}.dbo.finance_info_yearly " +
                $"INNER JOIN {Database}.dbo.finance_stat_yearly " +
                $"ON {Database}.dbo.finance_info_yearly.Symbol = {Database}.dbo.finance_stat_yearly.Symbol " +
                $"AND {Database}.dbo.finance_info_yearly.Year = {Database}.dbo.finance_stat_yearly.Year " +
                $"AND {Database}.dbo.finance_info_yearly.Symbol = '{stock.Symbol}' ";
            SqlCommand command = new SqlCommand(sql, cnn);
            command.Parameters.AddWithValue("@zip", "india");

            List<GS> netprofit = new List<GS>();
            GrowthStock growth_stock = new GrowthStock();
            growth_stock.Symbol = stock.Symbol;
            growth_stock.LastUpdate = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            using (SqlDataReader reader = command.ExecuteReader())
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
                stock.Growth_stock = ((Assets_rate + Net_rate + Price_rate) / 3).ToString();
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
            SqlConnection cnn;
            connetionString = $@"Data Source={DatabaseServer};Initial Catalog={Database};User ID={Username};Password={Password}";
            cnn = new SqlConnection(connetionString);
            cnn.Open();

            sql = $"Select * from dbo.{db} where {where}";
            SqlCommand command = new SqlCommand(sql, cnn);
            command.Parameters.AddWithValue("@zip", "india");
            using (SqlDataReader reader = command.ExecuteReader())
            {
                if (reader.Read())
                {
                    if (String.Format("{0}", reader["Average_TargetPrice"]) != "")
                    {
                        double last_price = Convert.ToDouble(String.Format("{0}", reader["LastPrice"]));
                        double target_price = Convert.ToDouble(String.Format("{0}", reader["Average_TargetPrice"]));
                        stock.Price = Math.Round((target_price - last_price) * 100 / last_price, 2).ToString();
                        double buy = Convert.ToInt32(String.Format("{0}", reader["Buy"]));
                        double hold = Convert.ToInt32(String.Format("{0}", reader["Hold"]));
                        double sell = Convert.ToInt32(String.Format("{0}", reader["Sell"]));
                        if ((buy + hold + sell) > 0)
                            stock.IAA = Math.Round((buy + (hold / 2)) * 100 / (buy + hold + sell), 2).ToString();
                    }
                }
            }

            cnn.Close();
        }
        private static void StatementDatabase(object item, string db, string where, bool none_update = false)
        {
            string sql = "";
            string connetionString;
            SqlConnection cnn;
            connetionString = $@"Data Source={DatabaseServer};Initial Catalog={Database};User ID={Username};Password={Password}";
            cnn = new SqlConnection(connetionString);
            cnn.Open();

            sql = $"Select * from dbo.{db} where {where}";
            SqlCommand command = new SqlCommand(sql, cnn);
            command.Parameters.AddWithValue("@zip", "india");
            bool event_case = true;
            using (SqlDataReader reader = command.ExecuteReader())
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
        private static void UpdateDatebase(string sql, SqlConnection cnn)
        {
            SqlCommand command;
            SqlDataAdapter adapter = new SqlDataAdapter();

            try
            {
                command = new SqlCommand(sql, cnn);
                adapter.UpdateCommand = new SqlCommand(sql, cnn);
                adapter.UpdateCommand.ExecuteNonQuery();
                command.Dispose();
            }
            catch (Exception ex)
            {
                log.LOGE($"[FundamentalSET100::UpdateDatebase]  {sql}");
            }
        }
        private static void InsertDatebase(string sql, SqlConnection cnn)
        {
            SqlCommand command;
            SqlDataAdapter adapter = new SqlDataAdapter();

            try
            {
                command = new SqlCommand(sql, cnn);
                adapter.InsertCommand = new SqlCommand(sql, cnn);
                adapter.InsertCommand.ExecuteNonQuery();
                command.Dispose();
            }
            catch (Exception ex)
            {
                log.LOGE($"[FundamentalSET100::InsertDatebase]  {sql}");
            }
        }
        private static string GetInsertSQL(object item, string db)
        {
            string sql = $"INSERT INTO dbo.{db} (:columns:) VALUES (:values:);";

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
            string sql = $"UPDATE dbo.{db} SET :update: WHERE {whare} ;";

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
            string columnnames = string.Join(",", columns);
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
                    result += $"{columns[i]} = '{values[i]}'" + (i + 1 != columns.Length ? ", " : " ");
                else
                    result += $"{columns[i]} = null" + (i + 1 != columns.Length ? ", " : " ");
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
