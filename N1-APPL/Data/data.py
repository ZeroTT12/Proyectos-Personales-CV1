import requests
import pandas as pd

# --------------------------------------------------
# 1. Descarga datos SEC
# --------------------------------------------------
def fetch_company_facts(cik: str) -> dict:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    headers = {
        "User-Agent": "Academic research (tuemail@dominio.com)"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


# --------------------------------------------------
# 2. Construcción DataFrame base US-GAAP
# --------------------------------------------------
def build_us_gaap_dataframe(data: dict, cik: str) -> pd.DataFrame:
    records = []

    for account, account_data in data.get("facts", {}).get("us-gaap", {}).items():
        units = account_data.get("units", {})
        if "USD" not in units:
            continue

        for item in units["USD"]:
            records.append({
                "cik": cik,
                "account": account,
                "start": item.get("start"),
                "end": item.get("end"),
                "value": item.get("val"),
                "form": item.get("form"),
                "fy": item.get("fy"),
                "fp": item.get("fp")
            })

    df = pd.DataFrame(records)

    df["start"] = pd.to_datetime(df["start"], errors="coerce")
    df["end"] = pd.to_datetime(df["end"], errors="coerce")
    df["fy"] = pd.to_numeric(df["fy"], errors="coerce")
    df["duracion_dias"] = (df["end"] - df["start"]).dt.days

    return df


# --------------------------------------------------
# 3. Income Statement (Statement of Operations)
# --------------------------------------------------
def get_income_statement(df: pd.DataFrame, years: list[int]) -> pd.DataFrame:
    income_accounts = [
        "SalesRevenueNet",
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "CostOfGoodsAndServicesSold",
        "GrossProfit",
        "OperatingExpenses",
        "ResearchAndDevelopmentExpense",
        "SellingGeneralAndAdministrativeExpense",
        "OperatingIncomeLoss",
        "NonoperatingIncomeExpense",
        "InterestExpense",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
        "IncomeTaxExpenseBenefit",
        "NetIncomeLoss"
    ]

    df_is = df[
        (df["form"] == "10-K") &
        (df["fp"] == "FY") &
        (df["duracion_dias"] > 300) &
        (df["fy"].isin(years)) &
        (df["account"].isin(income_accounts))
    ].copy()

    df_is = df_is.drop(columns=["cik", "start", "form", "fy", "fp", "duracion_dias"])

    df_is = df_is.pivot_table(
        index="end",
        columns="account",
        values="value",
        aggfunc="last"
    ).sort_index()

    return df_is


# --------------------------------------------------
# 4. Balance Sheet
# --------------------------------------------------
def get_balance_sheet(df: pd.DataFrame, years: list[int]) -> pd.DataFrame:
    balance_accounts = [
        "Assets",
        "AssetsCurrent",
        "AssetsNoncurrent",
        "CashAndCashEquivalentsAtCarryingValue",
        "MarketableSecuritiesCurrent",
        "MarketableSecuritiesNoncurrent",
        "AccountsReceivableNetCurrent",
        "InventoryNet",
        "PropertyPlantAndEquipmentNet",
        "OtherAssetsCurrent",
        "OtherAssetsNoncurrent",
        "Liabilities",
        "LiabilitiesCurrent",
        "LiabilitiesNoncurrent",
        "AccountsPayableCurrent",
        "OtherLiabilitiesCurrent",
        "OtherLiabilitiesNoncurrent",
        "LongTermDebt",
        "LongTermDebtCurrent",
        "LongTermDebtNoncurrent",
        "CommercialPaper",
        "StockholdersEquity",
        "CommonStocksIncludingAdditionalPaidInCapital",
        "RetainedEarningsAccumulatedDeficit",
        "AccumulatedOtherComprehensiveIncomeLossNetOfTax",
        "LiabilitiesAndStockholdersEquity"
    ]

    df_bs = df[
        (df["form"] == "10-K") &
        (df["fp"] == "FY") &
        (df["start"].isna()) &
        (df["fy"].isin(years)) &
        (df["account"].isin(balance_accounts))
    ].copy()

    df_bs = df_bs.drop(columns=["cik", "start", "form", "fy", "fp", "duracion_dias"])

    df_bs = df_bs.pivot_table(
        index="end",
        columns="account",
        values="value",
        aggfunc="last"
    ).sort_index()

    return df_bs


# --------------------------------------------------
# 5. DataFrame combinado
# --------------------------------------------------
def get_combined(df_is: pd.DataFrame, df_bs: pd.DataFrame) -> pd.DataFrame:
    df_combined = pd.concat(
        {
            "IncomeStatement": df_is,
            "BalanceSheet": df_bs
        },
        axis=1
    )
    return df_combined


# --------------------------------------------------
# 6. Pipeline completo
# --------------------------------------------------
def get_financial_statements(cik: str,
                             is_years: list[int],
                             bs_years: list[int]):

    data = fetch_company_facts(cik)
    df_base = build_us_gaap_dataframe(data, cik)

    df_income_statement = get_income_statement(df_base, is_years)
    df_balance_sheet = get_balance_sheet(df_base, bs_years)
    df_combined = get_combined(df_income_statement, df_balance_sheet)

    return df_income_statement, df_balance_sheet, df_combined