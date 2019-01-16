/*
Koje firme kasno odgovaraju na zahteve korisnika. Da li mozda one imaju i najveci broj zalbi?
*/

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

--------------------------------------- calculate number of late response per company
-- projection on propery company
lateComplaints = FILTER allComplaints BY timelyResponse == 'No'; 
lateCompanies = FOREACH lateComplaints GENERATE company, timelyResponse;
-- group by company name
lateCompaniesGroup = GROUP lateCompanies BY company;
-- count how many times each company name appers
lateCompaniesCount = FOREACH lateCompaniesGroup GENERATE group AS companyName:chararray, COUNT(lateCompanies.company) AS countLate:long;
-- sort by number of appearance
sortLateCompaniesDesc = ORDER lateCompaniesCount BY countLate DESC;

-------------------------------------- calculate number of company
-- projection on propery company
companies = FOREACH allComplaints GENERATE company;
-- group by company name
companiesGroup = GROUP companies BY company;
-- count how many times each company name appers
companiesCount = FOREACH companiesGroup GENERATE group AS companyName:chararray, COUNT(companies.company) AS count:long;
-- sort by number of appearance
sortCompaniesDesc = ORDER companiesCount BY count DESC;

-- join relations
joined = JOIN sortLateCompaniesDesc BY companyName, sortCompaniesDesc BY companyName;
-- calculate percentage
percentageLateResponse = FOREACH joined GENERATE sortLateCompaniesDesc::companyName, countLate, count, ((double)countLate/(double)count)*100 AS (percentage:double);
-- sort by percentage DESC
orderDescPercentageLateResponse = ORDER percentageLateResponse BY percentage DESC;
-- print to stdout
dump orderDescPercentageLateResponse;

/*
Output

Some has 100%, some less. Other has 0%.

*/
-- store
STORE orderDescPercentageLateResponse INTO '/home/complaints/percentageLateResponse' using CSVExcelStorage(',');	



/*
Da li nam se brojke sa JOINOM poklapaju
sortLateCompaniesDescAll = GROUP sortLateCompaniesDesc ALL;
countSortLateCompaniesDescAll = FOREACH sortLateCompaniesDescAll GENERATE COUNT(sortLateCompaniesDesc.countLate);
dump countSortLateCompaniesDescAll;
-- 3044

joinedAll = GROUP joined ALL;
countJoined = FOREACH joinedAll GENERATE COUNT(joined);
dump countJoined;
-- 3044
*/
