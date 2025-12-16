profit_mapping = {
    # 基本信息字段
    "证券代码": "Stkcd",
    "证券简称": "ShortName",
    "报表类型": "Typrep",
    "是否发生差错更正": "IfCorrect",
    "差错更正披露日期": "DeclareDate",
    
    # 营业收入相关
    "营业总收入": "TotOpRev",
    "营业收入": "OpRev",
    "利息净收入": "NetIntInc",
    "利息收入": "IntInc",
    "利息支出": "IntExp",
    "已赚保费": "EarnPrem",
    "保险业务收入": "PremInc",
    "其中：分保费收入": "CededPremInc",
    "减：分出保费": "LessCededPrem",
    "减：提取未到期责任准备金": "LessUnearnPremRes",
    "手续费及佣金净收入": "NetFeeCommInc",
    "其中：代理买卖证券业务净收入": "NetBrokInc",
    "其中:证券承销业务净收入": "NetUwrInc",
    "其中：受托客户资产管理业务净收入": "NetAssetMgmtInc",
    "手续费及佣金收入": "FeeCommInc",
    "手续费及佣金支出": "FeeCommExp",
    "其他业务收入": "OthBizInc",
    
    # 营业成本相关
    "营业总成本": "TotOpCost",
    "营业成本": "OpCost",
    "退保金": "SurrBene",
    "赔付支出净额": "NetClmExp",
    "赔付支出": "ClmExp",
    "减：摊回赔付支出": "LessRecClmExp",
    "提取保险责任准备金净额": "NetInsResExp",
    "提取保险责任准备金": "InsResExp",
    "减：摊回保险责任准备金": "LessRecInsRes",
    "保单红利支出": "PolDivExp",
    "分保费用": "CededExp",
    "税金及附加": "TaxSurchg",
    "业务及管理费": "BizMgmtExp",
    "减：摊回分保费用": "LessRecCededExp",
    "保险业务手续费及佣金支出": "InsFeeCommExp",
    "销售费用": "SellExp",
    "管理费用": "AdminExp",
    "研发费用": "RDExp",
    "财务费用": "FinExp",
    "其中：利息费用(财务费用)": "IntFinExp",
    "其中：利息收入(财务费用)": "IntFinInc",
    
    # 其他收益
    "其他收益": "OthGain",
    "投资收益": "InvInc",
    "其中：对联营企业和合营企业的投资收益": "InvIncAssocJV",
    "其中：以摊余成本计量的金融资产终止确认收益": "GainLossFVTPL",
    "汇兑收益": "ExchGain",
    "净敞口套期收益": "NetHedgeGain",
    "公允价值变动收益": "FVChangeGain",
    "资产减值损失": "AssetImpLoss",
    "信用减值损失": "CreditImpLoss",
    "资产处置收益": "AssetDispGain",
    "其他业务成本": "OthBizCost",
    "其他业务利润": "OthBizProfit",
    "营业利润": "OpProfit",
    
    # 营业外收支
    "加：营业外收入": "NonOpInc",
    "其中：非流动资产处置利得": "NonOpIncDisp",
    "减：营业外支出": "NonOpExp",
    "其中：非流动资产处置净损益": "NonOpExpDispNet",
    "其中：非流动资产处置损失": "NonOpExpDispLoss",
    
    # 利润总额及以下
    "利润总额": "ProfitBefTax",
    "减：所得税费用": "IncomeTax",
    "未确认的投资损失": "UncInvLoss2",
    "影响净利润的其他项目": "OthNetImpact",
    "净利润": "NetProfit",
    "持续经营净利润": "ContOpProfit",
    "终止经营净利润": "DiscOpProfit",
    "归属于母公司所有者的净利润": "ParNetProfit",
    "归属于母公司其他权益工具持有者的净利润": "ParOthEqNetProfit",
    "少数股东损益": "MinNetProfit",
    
    # 每股收益
    "基本每股收益": "BasicEPS",
    "稀释每股收益": "DilutedEPS",
    
    # 综合收益
    "其他综合收益(损失)": "OthComInc",
    "归属母公司所有者的其他综合收益的税后净额": "ParOthComInc",
    "归属于少数股东的其他综合收益的税后净额": "MinOthComInc",
    "综合收益总额": "TotComInc",
    "归属于母公司所有者的综合收益": "ParTotComInc",
    "归属于母公司其他权益工具持有者的综合收益总额": "ParOthEqTotComInc",
    "归属少数股东的综合收益": "MinTotComInc"
}

# 反向映射（字段名到COMMENT）
profit_mapping_revert = {v: k for k, v in profit_mapping.items()}

# 字段顺序列表（按照DDL中的顺序）
profit_order = [
    "Stkcd", "ShortName", "Accper", "Typrep", "IfCorrect", "DeclareDate",
    # 营业收入相关
    "TotOpRev", "OpRev", "NetIntInc", "IntInc", "IntExp", "EarnPrem", 
    "PremInc", "CededPremInc", "LessCededPrem", "LessUnearnPremRes", "NetFeeCommInc", 
    "NetBrokInc", "NetUwrInc", "NetAssetMgmtInc", "FeeCommInc", "FeeCommExp", 
    "OthBizInc",
    # 营业成本相关
    "TotOpCost", "OpCost", "SurrBene", "NetClmExp", "ClmExp", "LessRecClmExp", 
    "NetInsResExp", "InsResExp", "LessRecInsRes", "PolDivExp", "CededExp", 
    "TaxSurchg", "BizMgmtExp", "LessRecCededExp", "InsFeeCommExp", "SellExp", 
    "AdminExp", "RDExp", "FinExp", "IntFinExp", "IntFinInc",
    # 其他收益
    "OthGain", "InvInc", "InvIncAssocJV", "GainLossFVTPL", "ExchGain", "NetHedgeGain", 
    "FVChangeGain", "AssetImpLoss", "CreditImpLoss", "AssetDispGain", "OthBizCost", 
    "OthBizProfit", "OpProfit",
    # 营业外收支
    "NonOpInc", "NonOpIncDisp", "NonOpExp", "NonOpExpDispNet", "NonOpExpDispLoss",
    # 利润总额及以下
    "ProfitBefTax", "IncomeTax", "UncInvLoss2", "OthNetImpact", "NetProfit", 
    "ContOpProfit", "DiscOpProfit", "ParNetProfit", "ParOthEqNetProfit", "MinNetProfit",
    # 每股收益
    "BasicEPS", "DilutedEPS",
    # 综合收益
    "OthComInc", "ParOthComInc", "MinOthComInc", "TotComInc", "ParTotComInc", 
    "ParOthEqTotComInc", "MinTotComInc"
]

balance_mapping = {
    # 基本信息字段
    "证券代码": "Stkcd",
    "证券简称": "ShortName",
    "报表类型": "Typrep",
    "是否发生差错更正": "IfCorrect",
    "差错更正披露日期": "DeclareDate",
    
    # 流动资产
    "货币资金": "Cash",
    "其中:客户资金存款": "ClientCash",
    "结算备付金": "SettProvFund",
    "其中：客户备付金": "ClientProvFund",
    "现金及存放中央银行款项": "CashWithCB",
    "存放同业款项": "DueFromBanks",
    "贵金属": "PrecMetal",
    "拆出资金净额": "NetLendFund",
    "交易性金融资产": "TradFinAsset",
    "衍生金融资产": "DeriFinAsset",
    "短期投资净额": "ShortInvNet",
    "应收票据净额": "NotesRecNet",
    "应收账款净额": "AcctRecNet",
    "应收款项融资": "RecFin",
    "预付款项净额": "PrepayNet",
    "应收保费净额": "PremRecNet",
    "应收分保账款净额": "ReinsRecNet",
    "应收代位追偿款净额": "SubroRecNet",
    "应收分保合同准备金净额": "ReinsResRecNet",
    "其中:应收分保未到期责任准备金净额": "ReinsUnearnPremResRecNet",
    "其中:应收分保未决赔款准备金净额": "ReinsClmResRecNet",
    "其中:应收分保寿险责任准备金净额": "ReinsLifeResRecNet",
    "其中:应收分保长期健康险责任准备金净额": "ReinsLTHIResRecNet",
    "应收利息净额": "IntRecNet",
    "应收股利净额": "DivRecNet",
    "其他应收款净额": "OthRecNet",
    "买入返售金融资产净额": "RepoRecNet",
    "存货净额": "InventNet",
    "其中：数据资源（存货）": "InventDataRes",
    "合同资产": "ContractAsset",
    "持有待售资产": "HeldForSaleAsset",
    "一年内到期的非流动资产": "NonCurAsset1Y",
    "存出保证金": "DepMargin",
    "其他流动资产": "OthCurAsset",
    "流动资产合计": "TotCurAsset",
    
    # 非流动资产
    "保户质押贷款净额": "PolicyLoanNet",
    "定期存款": "TimeDep",
    "发放贷款及垫款净额": "LoanAdvNet",
    "债权投资": "DebtInv",
    "以摊余成本计量的金融资产": "AmortCostFinAsset",
    "可供出售金融资产净额": "AFSInvNet",
    "其他债权投资": "OthDebtInv",
    "以公允价值计量且其变动计入其他综合收益的债务工具投资": "FVTOCI_Debt",
    "持有至到期投资净额": "HTMInvNet",
    "长期应收款净额": "LTARecNet",
    "长期股权投资净额": "LTEInvNet",
    "其他权益工具投资": "OthEquityInv",
    "以公允价值计量且其变动计入其他综合收益的权益工具投资": "FVTOCI_Equity",
    "以公允价值计量且其变动计入其他综合收益的金融资产": "FVTOCI_FinAsset",
    "其他非流动金融资产": "OthNonCurFinAsset",
    "长期债权投资净额": "LTDebtInvNet",
    "长期投资净额": "LTInvNet",
    "存出资本保证金": "CapMarginDep",
    "独立账户资产": "SepAccAsset",
    "投资性房地产净额": "InvPropNet",
    "固定资产净额": "FixedAssetNet",
    "在建工程净额": "ConstInProgNet",
    "工程物资": "ConstrMat",
    "固定资产清理": "FixedAssetDisp",
    "生产性生物资产净额": "ProdBioAssetNet",
    "油气资产净额": "OilGasAssetNet",
    "使用权资产": "RightUseAsset",
    "无形资产净额": "IntangAssetNet",
    "其中:交易席位费": "TradSeatFee",
    "其中：数据资源（无形资产）": "IntangDataRes",
    "开发支出": "DevExp",
    "其中：数据资源（开发支出）": "DevDataRes",
    "商誉净额": "GoodwillNet",
    "长期待摊费用": "LTDefExp",
    "递延所得税资产": "DefTaxAsset",
    "代理业务资产": "AgencyAsset",
    "其他非流动资产": "OthNonCurAsset",
    "非流动资产合计": "TotNonCurAsset",
    
    # 资产总计
    "其他资产": "OthAsset",
    "资产总计": "TotAsset",
    
    # 流动负债
    "短期借款": "ShortBorrow",
    "其中:质押借款": "PledgeBorrow",
    "应付短期融资款": "ShortFinBond",
    "向中央银行借款": "BorrowFromCB",
    "吸收存款及同业存放": "DepositDue",
    "其中：同业及其他金融机构存放款项": "DueToBanks",
    "其中：吸收存款": "CustomerDeposit",
    "拆入资金": "BorrowFromFI",
    "交易性金融负债": "TradFinLia",
    "衍生金融负债": "DeriFinLia",
    "应付票据": "NotesPay",
    "应付账款": "AcctPay",
    "预收款项": "AdvFromCust",
    "合同负债": "ContractLia",
    "卖出回购金融资产款": "RepoLia",
    "应付手续费及佣金": "FeeCommPay",
    "应付职工薪酬": "EmpBenefitPay",
    "应交税费": "TaxPay",
    "应付利息": "IntPay",
    "应付股利": "DivPay",
    "应付赔付款": "ClmPay",
    "应付保单红利": "PolDivPay",
    "保户储金及投资款": "PolicyDeposit",
    "保险合同准备金": "InsContractRes",
    "其中:未到期责任准备金": "UnearnPremRes",
    "其中:未决赔款准备金": "OutstClmRes",
    "其中:寿险责任准备金": "LifeInsRes",
    "其中:长期健康险责任准备金": "LTHIRes",
    "其他应付款": "OthPay",
    "应付分保账款": "ReinsPay",
    "代理买卖证券款": "ClientBroker",
    "代理承销证券款": "AgentUwr",
    "预收保费": "PremAdv",
    "持有待售负债": "HeldForSaleLia",
    "一年内到期的非流动负债": "NonCurLia1Y",
    "其他流动负债": "OthCurLia",
    "递延收益-流动负债": "DefRevCur",
    "流动负债合计": "TotCurLia",
    
    # 非流动负债
    "长期借款": "LTBorrow",
    "独立账户负债": "SepAccLia",
    "应付债券": "BondPay",
    "租赁负债": "LeaseLia",
    "长期应付款": "LTAccPay",
    "长期应付职工薪酬": "LTEmpBenefit",
    "专项应付款": "SpecAccPay",
    "长期负债合计": "TotLTLia",
    "预计负债": "ProvLia",
    "代理业务负债": "AgencyLia",
    "递延所得税负债": "DefTaxLia",
    "其他非流动负债": "OthNonCurLia",
    "递延收益-非流动负债": "DefRevNonCur",
    "非流动负债合计": "TotNonCurLia",
    
    # 负债总计
    "其他负债": "OthLia",
    "负债合计": "TotLia",
    
    # 所有者权益
    "实收资本(或股本)": "ShareCap",
    "其他权益工具": "OthEquityInstr",
    "其中：优先股": "PrefShare",
    "其中：永续债": "PerpDebt",
    "其中：其他": "OthEquityInstrOth",
    "资本公积": "CapSurp",
    "减：库存股": "LessTreaStock",
    "盈余公积": "SurRes",
    "一般风险准备": "GenRiskRes",
    "未分配利润": "RetEar",
    "外币报表折算差额": "ExchDiff",
    "加：未确认的投资损失": "UncInvLoss",
    "交易风险准备": "TradRiskRes",
    "专项储备": "SpecRes",
    "其他综合收益": "OthComInc",
    "归属于母公司所有者权益合计": "ParOwnEquity",
    "少数股东权益": "MinInt",
    "所有者权益合计": "TotEquity",
    "负债与所有者权益总计": "LiaEquity"
}

# 反向映射（字段名到COMMENT）
balance_mapping_revert = {v: k for k, v in balance_mapping.items()}

# 字段顺序列表（按照DDL中的顺序）
balance_order = [
    "Stkcd", "ShortName", "Accper", "Typrep", "IfCorrect", "DeclareDate",
    # 流动资产
    "Cash", "ClientCash", "SettProvFund", "ClientProvFund", "CashWithCB", 
    "DueFromBanks", "PrecMetal", "NetLendFund", "TradFinAsset", "DeriFinAsset", 
    "ShortInvNet", "NotesRecNet", "AcctRecNet", "RecFin", "PrepayNet", 
    "PremRecNet", "ReinsRecNet", "SubroRecNet", "ReinsResRecNet", "ReinsUnearnPremResRecNet", 
    "ReinsClmResRecNet", "ReinsLifeResRecNet", "ReinsLTHIResRecNet", "IntRecNet", 
    "DivRecNet", "OthRecNet", "RepoRecNet", "InventNet", "InventDataRes", 
    "ContractAsset", "HeldForSaleAsset", "NonCurAsset1Y", "DepMargin", "OthCurAsset", 
    "TotCurAsset",
    # 非流动资产
    "PolicyLoanNet", "TimeDep", "LoanAdvNet", "DebtInv", "AmortCostFinAsset", 
    "AFSInvNet", "OthDebtInv", "FVTOCI_Debt", "HTMInvNet", "LTARecNet", 
    "LTEInvNet", "OthEquityInv", "FVTOCI_Equity", "FVTOCI_FinAsset", "OthNonCurFinAsset", 
    "LTDebtInvNet", "LTInvNet", "CapMarginDep", "SepAccAsset", "InvPropNet", 
    "FixedAssetNet", "ConstInProgNet", "ConstrMat", "FixedAssetDisp", "ProdBioAssetNet", 
    "OilGasAssetNet", "RightUseAsset", "IntangAssetNet", "TradSeatFee", "IntangDataRes", 
    "DevExp", "DevDataRes", "GoodwillNet", "LTDefExp", "DefTaxAsset", 
    "AgencyAsset", "OthNonCurAsset", "TotNonCurAsset",
    # 资产总计
    "OthAsset", "TotAsset",
    # 流动负债
    "ShortBorrow", "PledgeBorrow", "ShortFinBond", "BorrowFromCB", "DepositDue", 
    "DueToBanks", "CustomerDeposit", "BorrowFromFI", "TradFinLia", "DeriFinLia", 
    "NotesPay", "AcctPay", "AdvFromCust", "ContractLia", "RepoLia", 
    "FeeCommPay", "EmpBenefitPay", "TaxPay", "IntPay", "DivPay", 
    "ClmPay", "PolDivPay", "PolicyDeposit", "InsContractRes", "UnearnPremRes", 
    "OutstClmRes", "LifeInsRes", "LTHIRes", "OthPay", "ReinsPay", 
    "ClientBroker", "AgentUwr", "PremAdv", "HeldForSaleLia", "NonCurLia1Y", 
    "OthCurLia", "DefRevCur", "TotCurLia",
    # 非流动负债
    "LTBorrow", "SepAccLia", "BondPay", "LeaseLia", "LTAccPay", 
    "LTEmpBenefit", "SpecAccPay", "TotLTLia", "ProvLia", "AgencyLia", 
    "DefTaxLia", "OthNonCurLia", "DefRevNonCur", "TotNonCurLia",
    # 负债总计
    "OthLia", "TotLia",
    # 所有者权益
    "ShareCap", "OthEquityInstr", "PrefShare", "PerpDebt", "OthEquityInstrOth", 
    "CapSurp", "LessTreaStock", "SurRes", "GenRiskRes", "RetEar", 
    "ExchDiff", "UncInvLoss", "TradRiskRes", "SpecRes", "OthComInc", 
    "ParOwnEquity", "MinInt", "TotEquity", "LiaEquity"
]

cashflow_mapping = {
    # 基本信息字段
    "证券代码": "Stkcd",
    "证券简称": "ShortName",
    "报表类型": "Typrep",
    "是否发生差错更正": "IfCorrect",
    "差错更正披露日期": "DeclareDate",
    
    # 经营活动现金流入
    "销售商品、提供劳务收到的现金": "SaleServiceCash",
    "客户存款和同业存放款项净增加额": "CustDepositIncrease",
    "存放央行和同业款项净减少额": "CBandIBDecrease",
    "向中央银行借款净增加额": "CentralBankLoanIncrease",
    "向其他金融机构拆入资金净增加额": "FinInstLoanIncrease",
    "收到原保险合同保费取得的现金": "InsurancePremiumCash",
    "收到再保险业务现金净额": "ReinsuranceCash",
    "保户储金及投资款净增加额": "PolicyDepositIncrease",
    "处置交易性金融资产净增加额": "TradingAssetDisposal",
    "收取利息、手续费及佣金的现金": "InterestFeeCommission",
    "拆入资金净增加额": "BorrowedFundsIncrease",
    "回购业务资金净增加额": "RepoFundsIncrease",
    "拆出资金净减少额": "LendingFundsDecrease",
    "买入返售款项净减少额": "ReverseRepoDecrease",
    "收到的税费返还": "TaxRefund",
    "收到的其他与经营活动有关的现金": "OtherOpIn",
    "经营活动现金流入小计": "TotalOpIn",
    
    # 经营活动现金流出
    "购买商品、接受劳务支付的现金": "GoodsPurchased",
    "客户贷款及垫款净增加额": "CustLoanIncrease",
    "向中央银行借款净减少额": "CentralBankLoanDecrease",
    "存放中央银行和同业款项净增加额": "CBandIBIncrease",
    "支付原保险合同赔付款项的现金": "InsuranceClaimPayment",
    "支付利息、手续费及佣金的现金": "InterestFeePayment",
    "支付再保业务现金净额": "ReinsurancePayment",
    "保户储金及投资款净减少额": "PolicyDepositDecrease",
    "拆出资金净增加额": "LendingFundsIncrease",
    "买入返售款项净增加额": "ReverseRepoIncrease",
    "拆入资金净减少额": "BorrowedFundsDecrease",
    "卖出回购款项净减少额": "SellRepoDecrease",
    "支付保单红利的现金": "PolicyDividendPayment",
    "支付给职工以及为职工支付的现金": "EmpPayment",
    "支付的各项税费": "TaxPayment",
    "支付其他与经营活动有关的现金": "OtherOpOut",
    "经营活动现金流出小计": "TotalOpOut",
    "经营活动产生的现金流量净额": "NetOpCF",
    
    # 投资活动现金流量
    "收回投资收到的现金": "InvestRecovery",
    "取得投资收益收到的现金": "InvestIncome",
    "处置固定资产、无形资产和其他长期资产收回的现金净额": "AssetDisposal",
    "处置子公司及其他营业单位收到的现金净额": "SubDisposal",
    "收到的其他与投资活动有关的现金": "OtherInvIn",
    "投资活动产生的现金流入小计": "TotalInvIn",
    "购建固定资产、无形资产和其他长期资产支付的现金": "AssetPurchase",
    "投资支付的现金": "InvestPayment",
    "质押贷款净增加额": "PledgeLoanIncrease",
    "取得子公司及其他营业单位支付的现金净额": "SubAcquisition",
    "支付其他与投资活动有关的现金": "OtherInvOut",
    "投资活动产生的现金流出小计": "TotalInvOut",
    "投资活动产生的现金流量净额": "NetInvCF",
    
    # 筹资活动现金流量
    "吸收投资收到的现金": "InvestReceived",
    "吸收权益性投资收到的现金": "EquityInvest",
    "其中：子公司吸收少数股东投资收到的现金": "MinorityInvest",
    "发行债券收到的现金": "BondIssuance",
    "取得借款收到的现金": "LoanReceived",
    "收到其他与筹资活动有关的现金": "OtherFinIn",
    "筹资活动现金流入小计": "TotalFinIn",
    "偿还债务支付的现金": "DebtRepayment",
    "分配股利、利润或偿付利息支付的现金": "DividendInterest",
    "其中：子公司支付给少数股东的股利、利润": "MinorityDividend",
    "支付其他与筹资活动有关的现金": "OtherFinOut",
    "筹资活动现金流出小计": "TotalFinOut",
    "筹资活动产生的现金流量净额": "NetFinCF",
    
    # 其他现金流量项目
    "汇率变动对现金及现金等价物的影响": "ForexEffect",
    "其他对现金的影响": "OtherCashEffect",
    "现金及现金等价物净增加额": "CashIncrease",
    "期初现金及现金等价物余额": "BeginCash",
    "期末现金及现金等价物余额": "EndCash"
}

# 反向映射（字段名到COMMENT）
cashflow_mapping_revert = {v: k for k, v in cashflow_mapping.items()}

# 字段顺序列表（按照DDL中的顺序）
cashflow_order = [
    "Stkcd", "ShortName", "Accper", "Typrep", "IfCorrect", "DeclareDate",
    # 经营活动现金流入
    "SaleServiceCash", "CustDepositIncrease", "CBandIBDecrease", "CentralBankLoanIncrease",
    "FinInstLoanIncrease", "InsurancePremiumCash", "ReinsuranceCash", "PolicyDepositIncrease",
    "TradingAssetDisposal", "InterestFeeCommission", "BorrowedFundsIncrease", "RepoFundsIncrease",
    "LendingFundsDecrease", "ReverseRepoDecrease", "TaxRefund", "OtherOpIn", "TotalOpIn",
    # 经营活动现金流出
    "GoodsPurchased", "CustLoanIncrease", "CentralBankLoanDecrease", "CBandIBIncrease",
    "InsuranceClaimPayment", "InterestFeePayment", "ReinsurancePayment", "PolicyDepositDecrease",
    "LendingFundsIncrease", "ReverseRepoIncrease", "BorrowedFundsDecrease", "SellRepoDecrease",
    "PolicyDividendPayment", "EmpPayment", "TaxPayment", "OtherOpOut", "TotalOpOut", "NetOpCF",
    # 投资活动现金流量
    "InvestRecovery", "InvestIncome", "AssetDisposal", "SubDisposal", "OtherInvIn", "TotalInvIn",
    "AssetPurchase", "InvestPayment", "PledgeLoanIncrease", "SubAcquisition", "OtherInvOut",
    "TotalInvOut", "NetInvCF",
    # 筹资活动现金流量
    "InvestReceived", "EquityInvest", "MinorityInvest", "BondIssuance", "LoanReceived",
    "OtherFinIn", "TotalFinIn", "DebtRepayment", "DividendInterest", "MinorityDividend",
    "OtherFinOut", "TotalFinOut", "NetFinCF",
    # 其他现金流量项目
    "ForexEffect", "OtherCashEffect", "CashIncrease", "BeginCash", "EndCash"
]