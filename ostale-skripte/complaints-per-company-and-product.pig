-- set CSV parser, beacuse of commas that are between quotes
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- loading all complaints
allComplaints = LOAD '/home/complaints/complaints-valid.csv'
   USING CSVExcelStorage(',')
   as ( dateReceived: chararray,
        product: chararray,
        subProduct: chararray,
        issue: chararray,
        subIssue: chararray,
        consumerComplaintNarrative: chararray,
        companyPublicResponse: chararray,
        company: chararray,
        state: chararray,
        zipCode: chararray,
        tags: chararray,
        consumerConsentProvided: chararray,
        submittedVia: chararray,
        dateSentToCompany: chararray,
        companyResponseToConsumer: chararray,
        timelyResponse: chararray,
        consumerDisputed: chararray,
        complaintId: chararray);

-- projection on propery company
companiesAndProducts = FOREACH allComplaints GENERATE company, product; 
-- group by company name
companiesAndProductsGroup = GROUP companiesAndProducts BY (company, product);
-- count how many times each company name appers
companiesAndProductsCount = FOREACH companiesAndProductsGroup GENERATE group AS cp:(companyName:chararray, productName:chararray), COUNT(companiesAndProducts) AS count:long;
-- sort by number of appearance
sortCompaniesAndProductsDesc = ORDER companiesAndProductsCount BY count DESC;
-- remove tuple
sortCompaniesAndProductsDesc = FOREACH sortCompaniesAndProductsDesc generate cp.companyName AS companyName:chararray, cp.productName AS productName:chararray, count AS countCP:long;
-- print on terminal
DUMP sortCompaniesAndProductsDesc;
/*
Output:
First 10

(EQUIFAX, INC.,Credit reporting, credit repair services, or other personal consumer reports,51004)
(EQUIFAX, INC.,Credit reporting,48128)
(Experian Information Solutions Inc.,Credit reporting,45378)
(Experian Information Solutions Inc.,Credit reporting, credit repair services, or other personal consumer reports,42629)
(BANK OF AMERICA, NATIONAL ASSOCIATION,Mortgage,42608)
(TRANSUNION INTERMEDIATE HOLDINGS, INC.,Credit reporting, credit repair services, or other personal consumer reports,41344)
(TRANSUNION INTERMEDIATE HOLDINGS, INC.,Credit reporting,39821)
(WELLS FARGO & COMPANY,Mortgage,35527)
(OCWEN LOAN SERVICING LLC,Mortgage,26087)
(Navient Solutions, LLC.,Student loan,23390)
*/
STORE sortCompaniesAndProductsDesc INTO '/home/complaints/companiesAndProducts' USING CSVExcelStorage(',');


--------------------------------------------------------------------------------------------------------------------------
-- calculate everything for company
companies = FOREACH allComplaints GENERATE company;
-- group by company name
companiesGroup = GROUP companies BY company;
-- count how many times each company name appers
companiesCount = FOREACH companiesGroup GENERATE group AS companyName:chararray, COUNT(companies) AS count:long;
-- sort by number of appearance
sortCompaniesDesc = ORDER companiesCount BY count DESC;



-- join
joined = JOIN sortCompaniesAndProductsDesc BY companyName, sortCompaniesDesc BY companyName;

-- calculate percentage
percentage = FOREACH joined GENERATE sortCompaniesDesc::companyName, sortCompaniesAndProductsDesc::productName, countCP, count, ((double)countCP/(double)count)*100 AS percentage:double;
orderDescPercentage = ORDER percentage BY percentage DESC;
dump orderDescPercentage;
-- store results
STORE orderDescPercentage INTO '/home/complaints/percentageProductPerCompany' using CSVExcelStorage(',');

/*

equifax = FILTER orderDescPercentage BY companyName == 'EQUIFAX, INC.';
dump equifax;
Output for EQUIFAX INC

(EQUIFAX, INC.,Credit reporting, credit repair services, or other personal consumer reports,51004,100580,50.70988268045337)
(EQUIFAX, INC.,Credit reporting,48128,100580,47.85046728971963)
(EQUIFAX, INC.,Debt collection,1147,100580,1.140385762577053)
(EQUIFAX, INC.,Credit card or prepaid card,66,100580,0.06561940743686617)
(EQUIFAX, INC.,Mortgage,56,100580,0.05567707297673494)
(EQUIFAX, INC.,Consumer Loan,55,100580,0.05468283953072181)
(EQUIFAX, INC.,Credit card,46,100580,0.045734738516603694)
(EQUIFAX, INC.,Student loan,31,100580,0.03082123682640684)
(EQUIFAX, INC.,Vehicle loan or lease,16,100580,0.015907735136209984)
(EQUIFAX, INC.,Bank account or service,14,100580,0.013919268244183734)
(EQUIFAX, INC.,Payday loan, title loan, or personal loan,7,100580,0.006959634122091867)
(EQUIFAX, INC.,Checking or savings account,4,100580,0.003976933784052496)
(EQUIFAX, INC.,Other financial service,3,100580,0.0029827003380393717)
(EQUIFAX, INC.,Money transfer, virtual currency, or money service,2,100580,0.001988466892026248)
(EQUIFAX, INC.,Prepaid card,1,100580,9.94233446013124E-4)

*/





/* Check number before and after joined
-- count first
sortCompaniesAndProductsDescGroupAll = GROUP sortCompaniesAndProductsDesc ALL;
countSortCompaniesAndProductsDesc = FOREACH sortCompaniesAndProductsDescGroupAll GENERATE COUNT(sortCompaniesAndProductsDesc.companyName);
dump countSortCompaniesAndProductsDesc;
-- 12177 companies and product


-- count second
sortCompaniesDescGroupAll = GROUP sortCompaniesDesc ALL;
countSortCompaniesDesc = FOREACH sortCompaniesDescGroupAll GENERATE COUNT(sortCompaniesDesc.companyName);
dump countSortCompaniesDesc;
-- 5104 companies

-- count joined
joinedGroupAll = GROUP joined ALL;
joinedCount = FOREACH joinedGroupAll GENERATE COUNT(joined);
dump joinedCount;
-- 12177 joined
*/




