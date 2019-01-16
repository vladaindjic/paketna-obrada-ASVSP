-- depends on complaints-per-company-and-product.pig
-- first run complaints-per-company-and-product.pig and then this script

DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

companiesAndProductsPercentage = LOAD 'hdfs://namenode:8020/percentageProductPerCompany'
   USING CSVExcelStorage(',')
   as (companyName:chararray,
       productName:chararray,
       countCP:long,
       count:long,
       percentage:double);

companiesAndCreaditCard = FILTER companiesAndProductsPercentage BY (productName matches '.*[Cc][Rr][Ee][Dd][Ii][Tt].*[Cc][Aa][Rr][Dd]');

groupedByCompany = GROUP companiesAndCreaditCard BY companyName;
-- the reason why we remove productName is because we know that each productName contains Credit card (look previous FILTER statement)
companiesAndCreaditCard = FOREACH groupedByCompany {
	total_cp = SUM(companiesAndCreaditCard.countCP);
	total_percentage = SUM(companiesAndCreaditCard.percentage);
	total_count = MAX(companiesAndCreaditCard.count);  -- all are the same, so the MAX won't change anything
	GENERATE group AS (companyName:chararray), total_cp AS (countCP:long), total_count AS (count:long), total_percentage AS (percentage:double);
};

orderDescByCreditCardCount = ORDER companiesAndCreaditCard BY countCP DESC;
-- dump orderDescByCreditCardCount;

STORE orderDescByCreditCardCount INTO 'hdfs://namenode:8020/companiesAndCreditCardCount' USING CSVExcelStorage(',');
orderDescByCreditCardPercentage = ORDER companiesAndCreaditCard BY percentage DESC;
-- dump orderDescByCreditCardPercentage;
STORE orderDescByCreditCardPercentage INTO 'hdfs://namenode:8020/companiesAndCreditCardPercentage' USING CSVExcelStorage(',');

/*
    Output:

    --- before grouping with the same name
    (CITIBANK, N.A.,Credit card,16817,45765,36.74642193816235)
    (CAPITAL ONE FINANCIAL CORPORATION,Credit card,12920,31357,41.20292119781867)
    (JPMORGAN CHASE & CO.,Credit card,10373,56368,18.402284984388306)
    (BANK OF AMERICA, NATIONAL ASSOCIATION,Credit card,9111,79440,11.4690332326284)
    (SYNCHRONY FINANCIAL,Credit card,8779,19865,44.19330480745029)
    (AMERICAN EXPRESS COMPANY,Credit card,6669,12408,53.74758220502901)
    (CITIBANK, N.A.,Credit card or prepaid card,5566,45765,12.162132634109035)
    (CAPITAL ONE FINANCIAL CORPORATION,Credit card or prepaid card,4895,31357,15.610549478585323)
    (DISCOVER BANK,Credit card,4264,9476,44.997889404812156)
    (JPMORGAN CHASE & CO.,Credit card or prepaid card,4093,56368,7.261212035197275)



    -- after grouping, check JPMORGAN CHASE & CO.
    (CITIBANK, N.A.,22383,45765,48.908554572271385)
    (CAPITAL ONE FINANCIAL CORPORATION,17815,31357,56.813470676403995)
    (JPMORGAN CHASE & CO.,14466,56368,25.66349701958558)
    (SYNCHRONY FINANCIAL,12225,19865,61.540397684369495)
    (BANK OF AMERICA, NATIONAL ASSOCIATION,12057,79440,15.17749244712991)
    (AMERICAN EXPRESS COMPANY,9739,12408,78.48968407479046)
    (DISCOVER BANK,5778,9476,60.97509497678345)
    (WELLS FARGO & COMPANY,5579,67422,8.274747115184955)
    (BARCLAYS BANK DELAWARE,4937,6302,78.34020945731514)
    (U.S. BANCORP,3476,16127,21.55391579338997)

    Manje firme imaju 1 zlabu i 100%

*/